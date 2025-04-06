#!/usr/bin/env python3

import re
import uuid
import fitz  # PyMuPDF
import psycopg2
import boto3
import os
import asyncio
import concurrent.futures
from fastapi import FastAPI, UploadFile, File, HTTPException, status, Form, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from typing import Dict, List, Optional, Tuple, Any
import tempfile
from urllib.parse import urlparse
from datetime import datetime
import hashlib
import traceback

# Logging Konfiguration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Exam PDF Processor", version="1.0.0")

# Lade .env Datei beim Start
load_dotenv()

# --- Globaler In-Memory Speicher für Task-Status (NUR FÜR workers=1 geeignet!) ---
processing_tasks = {}

# Globale Variable für Dateinamen
current_pdf_filename = ""

# Verbesserte Konfigurationsklasse
class Config:
    def __init__(self):
        # Lade Umgebungsvariablen
        self.supabase_url = os.getenv("SUPABASE_URL", "").strip()
        self.supabase_key = os.getenv("SUPABASE_KEY", "").strip()
        
        # Validiere Supabase URL
        if not self._is_valid_supabase_url(self.supabase_url):
            raise ValueError(
                "Ungültige Supabase URL. Format sollte sein: "
                "https://<project>.supabase.co"
            )
            
        # Validiere Supabase Key
        if not self.supabase_key or len(self.supabase_key) < 20:
            raise ValueError(
                "Ungültiger Supabase Key. Bitte überprüfen Sie den API Key "
                "in Ihren Projekteinstellungen."
            )

        # MinIO Konfiguration
        self.minio_config = {
            "endpoint_url": os.getenv("MINIO_ENDPOINT_URL"),
            "aws_access_key_id": os.getenv("MINIO_ROOT_USER"),
            "aws_secret_access_key": os.getenv("MINIO_ROOT_PASSWORD"),
        }

    def _is_valid_supabase_url(self, url: str) -> bool:
        """Validiert das Format der Supabase URL"""
        if not url:
            return False
            
        try:
            parsed = urlparse(url)
            # Prüfe grundlegendes URL Format
            if not all([parsed.scheme, parsed.netloc]):
                return False
            # Prüfe auf HTTPS
            if parsed.scheme != "https":
                return False
            # Prüfe auf supabase.co Domain
            if not parsed.netloc.endswith("supabase.co"):
                return False
            return True
        except Exception:
            return False

    @property
    def supabase(self) -> Client:
        try:
            # Stelle sicher, dass die URL mit / endet
            url = self.supabase_url.rstrip("/")
            return create_client(url, self.supabase_key)
        except Exception as e:
            logger.error(f"Supabase Verbindungsfehler: {str(e)}")
            raise

def validate_pdf(file: UploadFile) -> bool:
    """Validiert die PDF-Datei"""
    if not file.filename.endswith('.pdf'):
        return False
    # Hier könnten weitere Validierungen hinzugefügt werden
    return True

async def process_pdf(pdf_path: str, config: Config, metadata: Dict) -> Dict:
    """Verarbeitet das PDF mit verbesserter Fehlerbehandlung und Performance-Optimierungen"""
    try:
        # Extrahiere Header aus Dateinamen (als Fallback, wenn keine Metadaten angegeben)
        extracted_exam_name, extracted_exam_year, extracted_exam_semester = extract_exam_header(pdf_path)
        
        # Verwende übergebene Metadaten mit Fallback auf extrahierte Werte
        exam_name = metadata.get("exam_name") or extracted_exam_name
        exam_year = metadata.get("exam_year") or extracted_exam_year
        exam_semester = metadata.get("exam_semester") or extracted_exam_semester
        default_subject = metadata.get("subject", "")
        
        logger.info(f"Verarbeite PDF: {exam_name} {exam_year} {exam_semester}")

        # Extrahiere und verarbeite Fragen
        questions = extract_questions_with_coords(pdf_path)
        if not questions:
            logger.warning("Keine Fragen im PDF gefunden")
            return {
                "status": "completed",  # Status ist abgeschlossen, auch wenn nichts gefunden wurde
                "success": False, # Explizit als nicht erfolgreich markieren
                "message": "Keine Fragen im PDF gefunden",
                "data": {
                    "exam_name": exam_name,
                    "questions_processed": 0,
                    "images_uploaded": 0,
                },
                "questions": [] # Leere Liste für Konsistenz
            }
            
        # Verarbeite alle Fragen parallel mit Batch-Verarbeitung für bessere Performance
        # Teile die Fragen in Gruppen von 20 für parallele Verarbeitung
        batch_size = 20
        for i in range(0, len(questions), batch_size):
            batch = questions[i:i+batch_size]
            # Verarbeite diesen Batch parallel
            for q in batch:
                parse_question_details(q)
                
        logger.info(f"{len(questions)} Fragen extrahiert und verarbeitet")

        # Verarbeite Bilder mit verbesserter Fehlerbehandlung und Performance
        images = []
        try:
            logger.info("Starte optimierte Bildextraktion")
            images = extract_images_with_coords(pdf_path)
            logger.info(f"Extraktion ergab {len(images)} Bilder")
            
            # Überprüfe die Bilder auf korrekte Struktur (optimiert)
            valid_images = []
            for i, img in enumerate(images):
                if isinstance(img, dict) and all(k in img for k in ["page", "bbox", "image_bytes", "image_ext"]):
                    # Prüfe, ob die Bilddaten tatsächlich vorhanden sind
                    if img["image_bytes"] and len(img["image_bytes"]) > 100:
                        # Stelle sicher, dass bbox ein gültiges Format hat
                        bbox = img.get("bbox")
                        if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
                            # Repariere ungültiges bbox-Format
                            img["bbox"] = [0, i * 100, 100, (i + 1) * 100]
                            logger.warning(f"Ungültiges bbox-Format für Bild {i} repariert")
                        valid_images.append(img)
                    else:
                        logger.warning(f"Bild {i} hat ungültige oder leere Bilddaten: {len(img.get('image_bytes', b''))} Bytes")
                else:
                    logger.warning(f"Bild {i} hat ungültiges Format: {type(img)}")
                    
            images = valid_images
            logger.info(f"{len(images)} gültige Bilder gefunden")
            
            # Ordne Bilder den Fragen zu
            if images and questions:
                try:
                    images = map_images_to_questions(questions, images)
                    logger.info(f"{len(images)} Bilder extrahiert und zugeordnet")
                except Exception as map_error:
                    logger.error(f"Fehler bei der Bildzuordnung: {str(map_error)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    
                    # Einfache Notfallzuordnung: Verteile Bilder auf Fragen
                    logger.warning("Verwende einfache Notfallzuordnung für Bilder")
                    for img_idx, img in enumerate(images):
                        if img_idx < len(questions):
                            q = questions[img_idx % len(questions)]
                            img["question_id"] = q.get("id")
                            # Erstelle einen Bildschlüssel
                            image_key = f"{q.get('id')}_{img.get('page', 0)}_fallback.{img.get('image_ext', 'jpg')}"
                            q["image_key"] = image_key
                            logger.info(f"Bild {img_idx} der Frage {q.get('question_number', '?')} zugeordnet (Notfallzuordnung)")
            else:
                logger.warning("Keine Bilder oder Fragen zum Zuordnen vorhanden")
            
        except Exception as e:
            logger.error(f"Fehler bei der Bildverarbeitung: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            images = []  # Setze auf leere Liste im Fehlerfall

        # Speichere Bilder in MinIO mit parallelisierter Verarbeitung
        bucket_name = "exam-images"
        successful_uploads = 0
        assigned_images = sum(1 for img in images if img.get("question_id") is not None)
        logger.info(f"Starte parallelen Upload von {assigned_images} Bildern in MinIO")
        
        # Prüfe, ob die Bilder korrekt zugeordnet wurden
        if assigned_images == 0 and images:
            logger.warning("Keine Bilder wurden Fragen zugeordnet. Versuche alternative Zuordnung.")
            # Fallback: Ordne alle Bilder der ersten Frage zu, wenn keine Zuordnung erfolgte
            if questions:
                first_question = questions[0]
                for img_idx, img in enumerate(images):
                    img["question_id"] = first_question.get("id")
                    page = img.get("page", 0)
                    
                    # Sicherer Zugriff auf bbox
                    bbox = img.get("bbox", [0, 0, 0, 0])
                    img_y = 0
                    if isinstance(bbox, (list, tuple)) and len(bbox) > 1:
                        img_y = bbox[1]
                    elif isinstance(bbox, int):
                        img_y = bbox
                        
                    # Sicherstellen, dass img_y numerisch ist
                    if not isinstance(img_y, (int, float)):
                        try:
                            img_y = float(img_y)
                        except (ValueError, TypeError):
                            img_y = 0
                        
                    image_key = f"{first_question.get('id')}_{page}_{int(float(img_y))}.{img.get('image_ext', 'jpg')}"
                    first_question["image_key"] = image_key
                    logger.info(f"Bild {img_idx} der ersten Frage zugeordnet (Notfall-Fallback)")
        
        # Performance-Optimierung: Initialisiere S3-Client einmal außerhalb der Schleife
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=config.minio_config["endpoint_url"],
                aws_access_key_id=config.minio_config["aws_access_key_id"],
                aws_secret_access_key=config.minio_config["aws_secret_access_key"],
            )
            
            # Stelle sicher, dass der Bucket existiert (einmal vor allen Uploads)
            try:
                s3_client.head_bucket(Bucket=bucket_name)
                logger.info(f"Bucket {bucket_name} existiert")
            except Exception:
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Bucket {bucket_name} erstellt")
                except Exception as create_error:
                    logger.error(f"Bucket-Erstellung fehlgeschlagen: {str(create_error)}")
        except Exception as s3_error:
            logger.error(f"Fehler bei S3-Client-Initialisierung: {str(s3_error)}")
            s3_client = None
        
        # Bereite Bildupload-Tasks für Supabase Storage vor
        upload_tasks = []
        for img_idx, img in enumerate(images):
            if img.get("question_id"):
                # Sichere Extraktion der Y-Koordinate
                bbox = img.get("bbox", [0, 0, 0, 0])
                img_y = 0
                if isinstance(bbox, (list, tuple)) and len(bbox) > 1:
                    img_y = bbox[1]
                elif isinstance(bbox, int):
                    img_y = bbox
                
                # Stelle sicher, dass img_y numerisch ist
                if not isinstance(img_y, (int, float)):
                    try:
                        img_y = float(img_y)
                    except (ValueError, TypeError):
                        img_y = 0
                
                filename = f"{img['question_id']}_{img['page']}_{int(float(img_y))}.{img['image_ext']}"
                
                # Prüfe Bilddaten
                if img.get("image_bytes") and len(img["image_bytes"]) >= 100:
                    upload_tasks.append({
                        "img_idx": img_idx,
                        "filename": filename,
                        "image_bytes": img["image_bytes"],
                        "content_type": f'image/{img.get("image_ext", "jpg")}',
                        "question_id": img["question_id"]
                    })
        
        # Versuche zuerst den asynchronen Upload
        try:
            # Führe parallele Uploads durch (in Batches für Kontrolle)
            batch_size = 5  # Anzahl paralleler Uploads
            for i in range(0, len(upload_tasks), batch_size):
                batch = upload_tasks[i:i+batch_size]
                
                # Verarbeite diesen Batch an Uploads
                upload_futures = []
                
                for task in batch:
                    # Asynchrone Funktion für Upload
                    upload_future = asyncio.ensure_future(upload_image_async(
                        config, 
                        task["image_bytes"], 
                        task["filename"], 
                        bucket_name, 
                        task["content_type"]
                    ))
                    upload_futures.append((task, upload_future))
                
                # Warte auf Fertigstellung aller Uploads in diesem Batch
                for task, future in upload_futures:
                    try:
                        result = await future
                        if result:
                            successful_uploads += 1
                            # Aktualisiere die zugehörige Frage mit dem Bildschlüssel
                            for q in questions:
                                if q["id"] == task["question_id"]:
                                    q["image_key"] = task["filename"]
                                    logger.info(f"Bild {task['filename']} mit Frage {q.get('question_number', '?')} verknüpft")
                                    break
                    except Exception as e:
                        logger.error(f"Fehler beim asynchronen Upload von {task['filename']}: {str(e)}")
        
        except Exception as async_error:
            logger.error(f"Fehler bei der asynchronen Upload-Methode: {str(async_error)}")
            logger.warning("Verwende den synchronen Upload-Fallback")
            
            # Fallback: Verwende synchronen Upload, wenn der asynchrone fehlschlägt
            if successful_uploads == 0 and len(upload_tasks) > 0:
                logger.info("Starte synchrone Uploads als Fallback")
                
                for task in upload_tasks:
                    try:
                        # Synchroner Upload als Fallback mit Supabase Storage
                        options = {
                            'content-type': task["content_type"],
                            'upsert': 'true'
                        }
                        response = config.supabase.storage.from_(bucket_name).upload(
                            task["filename"], 
                            task["image_bytes"], 
                            options
                        )
                        
                        logger.info(f"Synchroner Supabase-Upload erfolgreich: {task['filename']}")
                        successful_uploads += 1
                        
                        # Aktualisiere die zugehörige Frage mit dem Bildschlüssel
                        for q in questions:
                            if q["id"] == task["question_id"]:
                                q["image_key"] = task["filename"]
                                logger.info(f"Bild {task['filename']} mit Frage {q.get('question_number', '?')} verknüpft (synchron)")
                                break
                    except Exception as e:
                        logger.error(f"Fehler beim synchronen Supabase-Upload von {task['filename']}: {str(e)}")

        # Bereite Fragedaten für das Frontend auf (anstatt sie direkt in die Datenbank einzufügen)
        formatted_questions = []
        
        for q in questions:
            # Verwende extrahierte Werte mit Fallback auf übergebene Metadaten
            subject = q.get("subject") or default_subject
            
            formatted_question = {
                "id": q.get("id", str(uuid.uuid4())),
                "question": q.get("question", ""),
                "options": {
                    "A": q.get("option_a", ""),
                    "B": q.get("option_b", ""),
                    "C": q.get("option_c", ""),
                    "D": q.get("option_d", ""),
                    "E": q.get("option_e", "")
                },
                "correctAnswer": q.get("correct_answer", ""),
                "subject": subject,
                "comment": q.get("comment", ""),
                "difficulty": 3,  # Standardwert
                "semester": exam_semester,
                "year": exam_year,
                "image_key": q.get("image_key", "")
            }
            formatted_questions.append(formatted_question)

        return {
            "status": "completed", # Konsistent "completed" verwenden
            "success": True,  # Für Frontend-Kompatibilität
            "data": {
                "exam_name": exam_name,
                "images_uploaded": successful_uploads,
                "total_questions": len(questions),
                "total_images": len(images),
            },
            "questions": formatted_questions  # Neue Struktur für das Frontend
        }

    except Exception as e:
        logger.error(f"Fehler bei der PDF-Verarbeitung: {str(e)}")
        return {
            "status": "failed", # Konsistent "failed" verwenden
            "success": False,  # Für Frontend-Kompatibilität
            "message": str(e),
            "data": {},
            "questions": []
        }

async def upload_image_async(s3_client, image_bytes, filename, bucket_name, content_type):
    """
    Asynchrone Funktion zum Hochladen eines Bildes nach MinIO mit Fehlerbehandlung
    """
    if not s3_client:
        logger.error(f"Kein S3-Client für Upload vorhanden: {filename}")
        return False
        
    if not image_bytes or len(image_bytes) < 100:
        logger.error(f"Unzureichende Bilddaten für {filename}: {len(image_bytes) if image_bytes else 0} Bytes")
        return False
        
    try:
        logger.info(f"Starte asynchronen Upload: {filename} ({len(image_bytes)} Bytes)")
        
        # Führe den S3-Upload in einem ThreadPool aus, um das I/O nicht zu blockieren
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            # Verwende ein maximales Timeout für den Upload
            upload_timeout = 30  # 30 Sekunden Timeout
            
            # Definiere die Upload-Funktion für den Thread
            def do_upload():
                try:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=filename,
                        Body=image_bytes,
                        ContentType=content_type
                    )
                    return True
                except Exception as upload_error:
                    logger.error(f"Fehler im Thread beim Upload von {filename}: {str(upload_error)}")
                    return False
            
            # Asynchrone Ausführung des S3-Uploads mit Timeout
            try:
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(pool, do_upload),
                    timeout=upload_timeout
                )
                
                if result:
                    logger.info(f"Asynchroner Upload erfolgreich abgeschlossen: {filename}")
                    return True
                else:
                    logger.error(f"Asynchroner Upload fehlgeschlagen (Thread-Fehler): {filename}")
                    return False
            except asyncio.TimeoutError:
                logger.error(f"Timeout beim Upload von {filename} nach {upload_timeout} Sekunden")
                return False
            except Exception as exec_error:
                logger.error(f"Fehler bei ThreadPool-Ausführung für {filename}: {str(exec_error)}")
                return False
            
    except Exception as e:
        logger.error(f"Unbehandelter Fehler beim asynchronen Upload von {filename}: {str(e)}")
        return False

async def upload_image_async(config, image_bytes, filename, bucket_name, content_type):
    """
    Asynchrone Funktion zum Hochladen eines Bildes nach Supabase Storage mit Fehlerbehandlung
    """
    if not config or not config.supabase:
        logger.error(f"Keine Supabase-Konfiguration für Upload vorhanden: {filename}")
        return False
        
    if not image_bytes or len(image_bytes) < 100:
        logger.error(f"Unzureichende Bilddaten für {filename}: {len(image_bytes) if image_bytes else 0} Bytes")
        return False
        
    try:
        logger.info(f"Starte asynchronen Supabase-Upload: {filename} ({len(image_bytes)} Bytes)")
        
        # Führe den Supabase-Upload in einem ThreadPool aus, um das I/O nicht zu blockieren
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            # Verwende ein maximales Timeout für den Upload
            upload_timeout = 30  # 30 Sekunden Timeout
            
            # Definiere die Upload-Funktion für den Thread
            def do_upload():
                try:
                    # Verwende die Supabase Storage API
                    options = {
                        'content-type': content_type,
                        'upsert': 'true'  # Überschreibe falls die Datei existiert
                    }
                    response = config.supabase.storage.from_(bucket_name).upload(filename, image_bytes, options)
                    
                    # Prüfe auf Fehler in der Antwort
                    if hasattr(response, 'error') and response.error:
                        logger.error(f"Supabase Storage Upload-Fehler: {response.error}")
                        return False
                    
                    return True
                except Exception as upload_error:
                    logger.error(f"Fehler im Thread beim Supabase-Upload von {filename}: {str(upload_error)}")
                    return False
            
            # Asynchrone Ausführung des Supabase-Uploads mit Timeout
            try:
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(pool, do_upload),
                    timeout=upload_timeout
                )
                
                if result:
                    logger.info(f"Asynchroner Supabase-Upload erfolgreich abgeschlossen: {filename}")
                    return True
                else:
                    logger.error(f"Asynchroner Supabase-Upload fehlgeschlagen (Thread-Fehler): {filename}")
                    return False
            except asyncio.TimeoutError:
                logger.error(f"Timeout beim Supabase-Upload von {filename} nach {upload_timeout} Sekunden")
                return False
            except Exception as exec_error:
                logger.error(f"Fehler bei ThreadPool-Ausführung für Supabase-Upload von {filename}: {str(exec_error)}")
                return False
            
    except Exception as e:
        logger.error(f"Unbehandelter Fehler beim asynchronen Supabase-Upload von {filename}: {str(e)}")
        return False

@app.on_event("startup")
async def startup_event():
    """Überprüft beim Start alle erforderlichen Konfigurationen und initialisiert Verbindungen"""
    try:
        logger.info("Starte Anwendung und prüfe Konfigurationen...")
        config = Config()
        
        # Prüfe und initialisiere Verbindungen
        connections_ok = True
        
        # Teste Supabase-Verbindung mit Timeout
        logger.info(f"Verbinde mit Supabase: {config.supabase_url}")
        try:
            # Versuch mit Timeout für verbesserte Zuverlässigkeit
            import asyncio
            from concurrent.futures import ThreadPoolExecutor
            
            # Zeitmessung starten
            start_time = datetime.now()
            
            # Führe Supabase-Test in einem separaten Thread aus
            with ThreadPoolExecutor() as executor:
                future = executor.submit(lambda: config.supabase.table('questions').select("count").limit(1).execute())
                # Mit Timeout warten
                try:
                    data, count = await asyncio.get_event_loop().run_in_executor(
                        None, 
                        lambda: future.result(timeout=10)  # 10 Sekunden Timeout
                    )
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Supabase Verbindung erfolgreich getestet (in {elapsed:.2f}s)")
                except Exception as timeout_err:
                    logger.error(f"Supabase Verbindungstest Timeout nach 10 Sekunden")
                    connections_ok = False
        except Exception as e:
            logger.error(f"Supabase Verbindungstest fehlgeschlagen: {str(e)}")
            connections_ok = False

        # Teste MinIO-Verbindung mit Timeout
        try:
            logger.info("Verbinde mit MinIO...")
            start_time = datetime.now()
            
            # Erstelle S3-Client
            s3_client = boto3.client(
                "s3", 
                endpoint_url=config.minio_config["endpoint_url"],
                aws_access_key_id=config.minio_config["aws_access_key_id"],
                aws_secret_access_key=config.minio_config["aws_secret_access_key"],
                config=boto3.session.Config(connect_timeout=5, read_timeout=5)  # Timeouts hinzufügen
            )
            
            # Mit Timeout testen
            with ThreadPoolExecutor() as executor:
                future = executor.submit(s3_client.list_buckets)
                try:
                    buckets = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: future.result(timeout=10)  # 10 Sekunden Timeout
                    )
                    elapsed = (datetime.now() - start_time).total_seconds()
                    bucket_count = len(buckets.get('Buckets', []))
                    logger.info(f"MinIO Verbindung erfolgreich getestet: {bucket_count} Buckets gefunden (in {elapsed:.2f}s)")
                    
                    # Stelle sicher, dass der Haupt-Bucket existiert
                    bucket_name = "exam-images"
                    try:
                        s3_client.head_bucket(Bucket=bucket_name)
                        logger.info(f"Bucket {bucket_name} existiert bereits")
                    except Exception:
                        logger.info(f"Bucket {bucket_name} existiert nicht, erstelle ihn...")
                        try:
                            s3_client.create_bucket(Bucket=bucket_name)
                            logger.info(f"Bucket {bucket_name} erfolgreich erstellt")
                        except Exception as create_error:
                            logger.error(f"Fehler beim Erstellen des Buckets {bucket_name}: {str(create_error)}")
                            connections_ok = False
                except Exception as timeout_err:
                    logger.error(f"MinIO Verbindungstest Timeout nach 10 Sekunden")
                    connections_ok = False
        except Exception as e:
            logger.error(f"MinIO Verbindungstest fehlgeschlagen: {str(e)}")
            connections_ok = False

        # Prüfe Gesamtergebnis
        if connections_ok:
            logger.info("✅ Alle Verbindungen erfolgreich initialisiert")
        else:
            logger.warning("⚠️ Einige Verbindungstests sind fehlgeschlagen; die Anwendung wird gestartet, aber es könnten Probleme auftreten")
    
    except Exception as e:
        logger.critical(f"Startup-Fehler: {str(e)}")
        # Werfen wir einen Fehler, aber stürzen nicht ab - die Anwendung wird sich im degradierten Modus starten
        logger.critical("Anwendung startet im degradierten Modus - einige Funktionen könnten nicht verfügbar sein")

# Health-Check-Endpunkte
@app.get("/health", summary="Einfacher Health-Check-Endpunkt")
async def health_check():
    """Einfacher Health-Check-Endpunkt"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/health/detailed", summary="Ausführlicher Health-Check aller Komponenten")
async def detailed_health_check():
    """Prüft die Verbindung zu allen externen Diensten"""
    health_status = {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "supabase": {"status": "unknown"},
            "minio": {"status": "unknown"}
        }
    }
    
    # Prüfe Supabase
    try:
        config = Config()
        start_time = datetime.now()
        data, count = config.supabase.table('questions').select("count").limit(1).execute()
        elapsed = (datetime.now() - start_time).total_seconds()
        health_status["components"]["supabase"] = {
            "status": "ok",
            "response_time_ms": int(elapsed * 1000)
        }
    except Exception as e:
        health_status["components"]["supabase"] = {
            "status": "error",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Prüfe MinIO
    try:
        config = Config()
        start_time = datetime.now()
        s3_client = boto3.client(
            "s3", 
            endpoint_url=config.minio_config["endpoint_url"],
            aws_access_key_id=config.minio_config["aws_access_key_id"],
            aws_secret_access_key=config.minio_config["aws_secret_access_key"],
            config=boto3.session.Config(connect_timeout=2, read_timeout=2)
        )
        response = s3_client.list_buckets()
        elapsed = (datetime.now() - start_time).total_seconds()
        health_status["components"]["minio"] = {
            "status": "ok",
            "response_time_ms": int(elapsed * 1000),
            "buckets": len(response.get('Buckets', []))
        }
    except Exception as e:
        health_status["components"]["minio"] = {
            "status": "error",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    return health_status

@app.post("/upload", 
    summary="Verarbeitet eine PDF-Datei",
    response_description="Verarbeitungsstatus und Details")
async def upload_pdf(
    file: UploadFile = File(...),
    examName: str = Form(""),
    examYear: str = Form(""),
    examSemester: str = Form(""),
    subject: str = Form(""),
    userId: str = Form(""), # Receive userId
    visibility: str = Form("private"), # Receive visibility, default to private
    background_tasks: BackgroundTasks = None
) -> JSONResponse:
    global current_pdf_filename
    current_pdf_filename = file.filename  # Speichere den Originalnamen
    
    # Validiere Metadaten
    if not examName:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "error",
                "success": False,
                "message": "Prüfungsname ist erforderlich",
                "data": {}
            }
        )
        
    # Validiere examYear, falls angegeben
    if examYear and not (examYear.isdigit() and len(examYear) == 4):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "error",
                "success": False,
                "message": "Prüfungsjahr muss ein vierstelliges Jahr sein",
                "data": {}
            }
        )
        
    # Validiere examSemester, falls angegeben
    if examSemester and examSemester not in ["WS", "SS"]:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "error",
                "success": False,
                "message": "Semester muss entweder 'WS' oder 'SS' sein",
                "data": {}
            }
        )
    
    try:
        if not validate_pdf(file):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Ungültige PDF-Datei"
            )

        # Verbesserte temporäre Dateibehandlung
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, f"upload_{uuid.uuid4()}.pdf")
        
        logger.info(f"Speichere PDF '{current_pdf_filename}' in temporäre Datei: {temp_file_path}")
        
        # Datei speichern und Dateihandle sofort schließen
        contents = await file.read()
        with open(temp_file_path, "wb") as f:
            f.write(contents)
            
        # Überprüfen, ob die Datei existiert
        if not os.path.exists(temp_file_path):
            raise FileNotFoundError(f"Temporäre Datei konnte nicht erstellt werden: {temp_file_path}")
            
        logger.info(f"Datei erfolgreich gespeichert: {os.path.getsize(temp_file_path)} Bytes")

        config = Config()
        global processing_tasks # Declare processing_tasks as global
        
        # Erstelle ein Metadaten-Wörterbuch für die PDF-Verarbeitung
        metadata = {
            "exam_name": examName,
            "exam_year": examYear,
            "exam_semester": examSemester,
            "subject": subject,
            "user_id": userId, # Pass userId to metadata
            "visibility": visibility # Pass visibility to metadata
        }
        
        # Erstelle eine Task-ID für das Tracking
        task_id = str(uuid.uuid4())
        
        # Initialisiere den Task-Status
        processing_tasks[task_id] = {
            "status": "processing",
            "message": "PDF-Verarbeitung gestartet",
            "data": {
                "exam_name": examName,
                "filename": file.filename
            }
        }
        
        # Im asynchronen Modus starten wir die Verarbeitung im Hintergrund
        # und geben sofort eine Antwort zurück
        background_tasks.add_task(
            process_pdf_in_background, 
            task_id, 
            temp_file_path, 
            config, 
            metadata
        )
        
        # Verzögere das Löschen der Datei
        background_tasks.add_task(cleanup_temp_file, temp_file_path, 3600)
        
        logger.info(f"Hintergrundaufgabe gestartet: {task_id}")
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "success": True,
                "status": "processing",
                "message": "PDF-Verarbeitung gestartet",
                "task_id": task_id
            }
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Upload-Fehler: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/status/{task_id}", 
    summary="Prüft den Status einer Verarbeitungsaufgabe",
    response_description="Status einer PDF-Verarbeitungsaufgabe")
async def check_task_status(task_id: str) -> JSONResponse:
    """
    Prüft den Status einer Verarbeitungsaufgabe anhand der Task-ID.
    
    - task_id: Die eindeutige ID der Aufgabe
    
    Gibt den aktuellen Status und ggf. die Ergebnisse zurück.
    """
    logger.info(f"Status-Abfrage für Task: {task_id}")
    global processing_tasks # Declare processing_tasks as global
    
    if task_id in processing_tasks:
        task_status = processing_tasks[task_id]
        
        # Bereite die Antwort vor
        response_content = {
            "success": task_status["status"] != "error",
            "status": task_status["status"],
            "message": task_status["message"]
        }
        
        # Füge Fragen und Daten hinzu, wenn verfügbar
        if "questions" in task_status:
            response_content["questions"] = task_status["questions"]
        
        if "data" in task_status:
            response_content["data"] = task_status["data"]
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_content
        )
    else:
        logger.warning(f"Task nicht gefunden: {task_id}")
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "success": False,
                "status": "error",
                "message": f"Aufgabe mit ID {task_id} nicht gefunden"
            }
        )

async def process_pdf_in_background(task_id: str, pdf_path: str, config: Config, metadata: Dict):
    """
    Verarbeitet das PDF im Hintergrund und aktualisiert den Task-Status.
    """
    try:
        global processing_tasks # Declare processing_tasks as global
        logger.info(f"Starte Hintergrundverarbeitung für Task {task_id}: {pdf_path}")
        
        # 1. Verarbeite die PDF-Datei (Extraktion, Bild-Upload)
        processing_result = await process_pdf(pdf_path, config, metadata)
        
        # 2. Prüfe Ergebnis der Verarbeitung
        if processing_result.get("status") == "completed" and processing_result.get("success") == True:
            logger.info(f"PDF Verarbeitung für Task {task_id} erfolgreich. Starte DB Insert.")
            formatted_questions = processing_result.get("questions", [])
            processing_data = processing_result.get("data", {})

            if not formatted_questions:
                # Fall: Verarbeitung erfolgreich, aber keine Fragen gefunden
                logger.warning(f"Task {task_id}: PDF verarbeitet, aber keine Fragen gefunden.")
                processing_tasks[task_id] = {
                    "status": "completed",
                    "success": False, # Nicht erfolgreich, da nichts zu speichern
                    "message": "PDF verarbeitet, aber keine Fragen gefunden.",
                    "data": processing_data
                }
            else:
                try:
                    # 3. Füge extrahierte Fragen in die DB ein
                    exam_name = metadata.get("exam_name", processing_data.get("exam_name", "Unknown"))
                    exam_year = metadata.get("exam_year", "")
                    exam_semester = metadata.get("exam_semester", "")
                    user_id = metadata.get("user_id", None) # Get userId from metadata
                    visibility = metadata.get("visibility", "private") # Get visibility

                    successful_inserts, failed_inserts = insert_questions_into_db(
                        formatted_questions, exam_name, exam_year, exam_semester, user_id, visibility, config
                    )

                    # 4. Aktualisiere Task-Status basierend auf DB-Ergebnis
                    if failed_inserts == 0 and successful_inserts > 0:
                        logger.info(f"Task {task_id}: {successful_inserts} Fragen erfolgreich in DB gespeichert.")
                        processing_tasks[task_id] = {
                            "status": "completed",
                            "success": True,
                            "message": f"{successful_inserts} Fragen erfolgreich verarbeitet und gespeichert.",
                            "data": processing_data # Behalte Verarbeitungs-Stats
                        }
                    elif successful_inserts > 0 and failed_inserts > 0:
                        logger.warning(f"Task {task_id}: DB Insert teilweise erfolgreich ({successful_inserts} OK, {failed_inserts} Failed).")
                        processing_tasks[task_id] = {
                            "status": "completed", # Abgeschlossen, aber nicht voll erfolgreich
                            "success": False, 
                            "message": f"Verarbeitung abgeschlossen, aber nur {successful_inserts} von {len(formatted_questions)} Fragen konnten gespeichert werden.",
                            "data": processing_data
                        }
                    else: # failed_inserts > 0 and successful_inserts == 0
                        logger.error(f"Task {task_id}: DB Insert komplett fehlgeschlagen ({failed_inserts} Failed).")
                        processing_tasks[task_id] = {
                            "status": "failed", # Fehler beim Speichern
                            "success": False,
                            "message": "PDF verarbeitet, aber Speichern der Fragen fehlgeschlagen.",
                            "data": processing_data
                        }
                except Exception as db_error:
                    logger.error(f"Fehler beim DB Insert für Task {task_id}: {str(db_error)}")
                    processing_tasks[task_id] = {
                        "status": "failed",
                        "success": False,
                        "message": f"Fehler beim Speichern der Fragen in der Datenbank: {str(db_error)}",
                        "data": processing_result.get("data", {}) 
                    }

        elif processing_result.get("status") == "completed" and processing_result.get("success") == False:
            # Fall: Verarbeitung selbst war nicht erfolgreich (z.B. keine Fragen gefunden in process_pdf)
            logger.warning(f"Task {task_id}: PDF Verarbeitung abgeschlossen, aber nicht erfolgreich.")
            processing_tasks[task_id] = {
                "status": "completed",
                "success": False,
                "message": processing_result.get("message", "PDF Verarbeitung nicht erfolgreich."),
                "data": processing_result.get("data", {})
            }
        else: # Verarbeitungsfehler (status: failed)
            logger.error(f"PDF Verarbeitung für Task {task_id} fehlgeschlagen.")
            processing_tasks[task_id] = {
                "status": "failed",
                "success": False,
                "message": processing_result.get("message", "Fehler bei der PDF-Verarbeitung."),
                "data": processing_result.get("data", {})
            }
            
    except Exception as e:
        logger.error(f"Schwerwiegender Fehler bei der Hintergrundverarbeitung von Task {task_id}: {str(e)}")
        # Aktualisiere den Status auf Fehler
        processing_tasks[task_id] = {
            "status": "failed", # Konsistent "failed" verwenden
            "success": False,
            "message": f"Fehler bei der Verarbeitung: {str(e)}",
            "data": {
                "error_details": str(e)
            }
         }
        # Eventuelle Aufräumarbeiten durchführen

async def cleanup_temp_file(file_path: str, delay_seconds: int = 0):
    """Löscht temporäre Dateien mit optionaler Verzögerung"""
    try:
        if delay_seconds > 0:
            import asyncio
            await asyncio.sleep(delay_seconds)
            
        if os.path.exists(file_path):
            os.unlink(file_path)
            logger.info(f"Temporäre Datei gelöscht: {file_path}")
        else:
            logger.warning(f"Temporäre Datei nicht gefunden beim Aufräumen: {file_path}")
    except Exception as e:
        logger.error(f"Fehler beim Löschen der temporären Datei {file_path}: {str(e)}")

def extract_exam_header(pdf_path):
    """
    Extrahiert Prüfungsname, Jahr und Semester aus dem Dateinamen.
    Erwartet ein Format wie "Biochemie_2022_WS.pdf"
    """
    filename = os.path.splitext(os.path.basename(pdf_path))[0]
    logger.info(f"Extrahiere Metadaten aus Dateiname: {filename}")
    
    try:
        return filename, "", ""
        
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren der Metadaten aus dem Dateinamen: {str(e)}")
        return filename, "", ""

def extract_questions_with_coords(pdf_path):
    """
    Extrahiert Fragen und ordnet Bilder direkt innerhalb ihrer Textblöcke (durch '_{10,}' getrennt) zu.
    """
    logger.info(f"Extrahiere Fragen und zugehörige Bilder aus PDF: {pdf_path}")
    
    try:
        doc = fitz.open(pdf_path)
        logger.info(f"PDF hat {len(doc)} Seiten")
    except Exception as e:
        logger.error(f"Konnte PDF nicht öffnen: {pdf_path} - Fehler: {e}")
        return []

    # Füge den gesamten Text zusammen
    full_text = ""
    page_texts = [] # Speichere Seitentexte und deren Startindex im Gesamttext
    current_index = 0
    for page_idx, page in enumerate(doc):
        try:
            page_text = page.get_text("text", sort=False) # Behalte Reihenfolge bei
            page_texts.append({"index": current_index, "text": page_text, "page_idx": page_idx})
            full_text += page_text
            current_index += len(page_text)
        except Exception as e_page_text:
             logger.error(f"Fehler beim Extrahieren von Text auf Seite {page_idx}: {e_page_text}")

    # Debug-Ausgabe eines Textausschnitts
    logger.info(f"Textprobe (erste 500 Zeichen): {full_text[:500]}")
    
    # Trenne Text in Fragen-Blöcke mit mindestens 10 Unterstrichen
    # Verwende finditer, um Start/Ende der Trenner zu bekommen
    separators = list(re.finditer(r'_{10,}', full_text))
    question_blocks_info = []
    last_end = 0
    for sep in separators:
        start, end = sep.span()
        block_text = full_text[last_end:start].strip()
        if block_text:
             question_blocks_info.append({"text": block_text, "start": last_end, "end": start})
        last_end = end
    # Letzten Block hinzufügen
    final_block_text = full_text[last_end:].strip()
    if final_block_text:
         question_blocks_info.append({"text": final_block_text, "start": last_end, "end": len(full_text)})

    logger.info(f"Gefunden: {len(question_blocks_info)} durch Unterstriche getrennte Textblöcke")
    
    questions = []
    
    # Globaler Cache für extrahierte Bild-XRefs, um Duplikate über Blöcke hinweg zu vermeiden
    extracted_xrefs_global = set() 

    # Gehe durch alle Blöcke
    for block_idx, block_info in enumerate(question_blocks_info):
        block_text = block_info["text"]
        block_start_index = block_info["start"]
        block_end_index = block_info["end"]
        
        # Extrahiere die Fragenummer und den Fragetext
        # ALT: question_pattern = re.compile(r'(\\d+)\\.\\s*Frage:?\\s*(.*?)(?=(?:\\s*[A-E]\\)|\\s*Fach:|\\s*Antwort:|\\s*Kommentar:|$))', re.DOTALL)
        # ALT: question_match = question_pattern.search(block_text)
        
        question_data = {
            "id": str(uuid.uuid4()),
            "page": -1, # Wird später gesetzt
            "y": 0,     # Wird später gesetzt
            "full_text": block_text, # Behalte den Blocktext für Debugging
            "question_number": str(block_idx + 1), # Fallback Nummerierung
            "question": "",
            "option_a": "", "option_b": "", "option_c": "", "option_d": "", "option_e": "",
            "subject": "", "correct_answer": "", "comment": "",
            "image_key": None, # Platzhalter für Bild
            "image_bytes": None,
            "image_ext": None
        }

        # --- NEUE LOGIK zur Erkennung von Frage, Optionen und Metadaten ---
        
        # 1. Finde Question Number und Start
        question_start_match = re.search(r'^(\\d+)\\.\\s*Frage:?', block_text, re.MULTILINE)
        
        if question_start_match:
            question_data["question_number"] = question_start_match.group(1)
            question_start_offset = question_start_match.end()

            # 2. Finde Ende des Question Body (Beginn der Metadaten am Ende des Blocks)
            # Suche nach Fach/Antwort/Kommentar, die typischerweise am Ende stehen
            metadata_start_offset = len(block_text) # Default: Ende des Blocks
            # Suche von hinten nach dem frühesten Metadaten-Tag
            for keyword in ["Fach:", "Antwort:", "Kommentar:"]:
                 try:
                     # re.IGNORECASE hinzugefügt
                     match_offset = block_text.rindex(keyword, 0, len(block_text), re.IGNORECASE) 
                     # Finde den Zeilenanfang des Keywords
                     line_start_offset = block_text.rfind('\n', 0, match_offset) + 1
                     metadata_start_offset = min(metadata_start_offset, line_start_offset)
                 except ValueError:
                     pass # Keyword nicht gefunden

            full_question_body = block_text[question_start_offset:metadata_start_offset].strip()
            metadata_body = block_text[metadata_start_offset:].strip()

            # 3. Extrahiere A-E Optionen vom ENDE des full_question_body
            options_text = ""
            question_text_final = full_question_body # Default: alles ist Frage
            
            lines = full_question_body.split('\n')
            option_lines_indices = []
            # Suche rückwärts nach dem Block der Optionen A-E
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].strip()
                if re.match(r'^[A-E]\)', line):
                    option_lines_indices.insert(0, i) # Füge am Anfang hinzu, um Reihenfolge zu wahren
                elif option_lines_indices and line: # Wenn wir Optionen gefunden haben und auf eine nicht-leere Nicht-Optionszeile stoßen
                    break # Der Block der Optionen ist zu Ende
                elif not option_lines_indices and line: # Wenn wir noch keine Optionen gefunden haben und auf Text stoßen
                     pass # Weitersuchen nach oben
            
            if option_lines_indices:
                first_option_line_index = option_lines_indices[0]
                options_text = '\n'.join(lines[first_option_line_index:]).strip()
                question_text_final = '\n'.join(lines[:first_option_line_index]).strip() # Text vor den Optionen
                
                # Extrahiere individuelle Optionen aus options_text
                for letter in "ABCDE":
                    # Regex sucht nach A), B) etc. am Zeilenanfang im Optionsblock
                    option_match = re.search(rf'^{letter}\\)\\s*(.*?)(?=\\s*(?:^\\s*[A-E]\\)|$)', options_text, re.MULTILINE | re.DOTALL)
                    if option_match:
                        question_data[f"option_{letter.lower()}"] = option_match.group(1).strip()
            else:
                # Keine A-E Optionen am Ende gefunden, versuche Fallback (weniger zuverlässig)
                 logger.warning(f"Keine A)-E) Optionen am Ende von Frage {question_data['question_number']} gefunden. Suche im gesamten Body.")
                 for letter in "ABCDE":
                     option_match = re.search(rf'{letter}\\)\\s*(.*?)(?=\\s*(?:[A-E]\\)|Fach:|Antwort:|Kommentar:)|$)', full_question_body, re.DOTALL)
                     if option_match:
                         question_data[f"option_{letter.lower()}"] = option_match.group(1).strip()

            question_data["question"] = question_text_final
            # Bereite Vorschau für Log vor (ersetze Newlines außerhalb des f-strings)
            question_body_preview = question_text_final[:80].replace('\n', ' ')
            logger.info(f"Block {block_idx+1}: Frage {question_data['question_number']} erkannt. Body bis Optionen: '{question_body_preview}...' Optionen gefunden: {bool(options_text)}")

            # 4. Extrahiere Metadaten aus metadata_body
            fach_match = re.search(r'Fach:\\s*(.*?)(?=\\s*Antwort:|\\s*Kommentar:|$)', metadata_body, re.DOTALL | re.IGNORECASE)
            if fach_match: question_data["subject"] = fach_match.group(1).strip()
            
            antwort_match = re.search(r'Antwort:\\s*(.*?)(?=\\s*Fach:|\\s*Kommentar:|$)', metadata_body, re.DOTALL | re.IGNORECASE)
            if antwort_match: question_data["correct_answer"] = antwort_match.group(1).strip()
            
            kommentar_match = re.search(r'Kommentar:\\s*(.*?)(?=\\s*Fach:|\\s*Antwort:|$)', metadata_body, re.DOTALL | re.IGNORECASE)
            if kommentar_match: question_data["comment"] = kommentar_match.group(1).strip()

        else:
             # --- Fallback / Alternative Frageerkennung (wenn "X. Frage:" nicht am Anfang steht) ---
             # Behalte die bisherige Logik für alternative Formate bei
             alt_match = re.search(r'(?:Was|Welche|Wo|Wann|Wie|Warum).*?\\?', block_text, re.DOTALL | re.IGNORECASE)
             if alt_match:
                 question_data["question"] = alt_match.group(0).strip()
                 logger.info(f"Block {block_idx+1}: Alternative Frage (?) gefunden: {question_data['question'][:60]}...")
                 # Extrahiere Optionen/Meta auch hier (vereinfacht)
                 for letter in "ABCDE":
                     option_match = re.search(rf'{letter}\\)\\s*(.*?)(?=\\s*(?:[A-E]\\)|Fach:|Antwort:|Kommentar:)|$)', block_text, re.DOTALL)
                     if option_match: question_data[f"option_{letter.lower()}"] = option_match.group(1).strip()
                 fach_match = re.search(r'Fach:\\s*(.*?)(?=\\s*Antwort:|\\s*Kommentar:|$)', block_text, re.DOTALL | re.IGNORECASE)
                 if fach_match: question_data["subject"] = fach_match.group(1).strip()
                 antwort_match = re.search(r'Antwort:\\s*(.*?)(?=\\s*Fach:|\\s*Kommentar:|$)', block_text, re.DOTALL | re.IGNORECASE)
                 if antwort_match: question_data["correct_answer"] = antwort_match.group(1).strip()
                 kommentar_match = re.search(r'Kommentar:\\s*(.*?)(?=\\s*Fach:|\\s*Antwort:|$)', block_text, re.DOTALL | re.IGNORECASE)
                 if kommentar_match: question_data["comment"] = kommentar_match.group(1).strip()
             else: 
                 first_line = block_text.split('\n')[0].strip()
                 if not re.match(r'^(Fach|Antwort|Kommentar):', first_line, re.IGNORECASE):
                     question_data["question"] = first_line
                     logger.info(f"Block {block_idx+1}: Fallback-Frage (erste Zeile) verwendet: {question_data['question'][:60]}...")
                     # Extrahiere Optionen/Meta auch hier (vereinfacht)
                     for letter in "ABCDE":
                        option_match = re.search(rf'{letter}\\)\\s*(.*?)(?=\\s*(?:[A-E]\\)|Fach:|Antwort:|Kommentar:)|$)', block_text, re.DOTALL)
                        if option_match: question_data[f"option_{letter.lower()}"] = option_match.group(1).strip()
                     fach_match = re.search(r'Fach:\\s*(.*?)(?=\\s*Antwort:|\\s*Kommentar:|$)', block_text, re.DOTALL | re.IGNORECASE)
                     if fach_match: question_data["subject"] = fach_match.group(1).strip()
                     antwort_match = re.search(r'Antwort:\\s*(.*?)(?=\\s*Fach:|\\s*Kommentar:|$)', block_text, re.DOTALL | re.IGNORECASE)
                     if antwort_match: question_data["correct_answer"] = antwort_match.group(1).strip()
                     kommentar_match = re.search(r'Kommentar:\\s*(.*?)(?=\\s*Fach:|\\s*Antwort:|$)', block_text, re.DOTALL | re.IGNORECASE)
                     if kommentar_match: question_data["comment"] = kommentar_match.group(1).strip()
                 else:
                     logger.warning(f"Block {block_idx+1}: Konnte keine klare Frage extrahieren.")
                     # Frage bleibt leer, aber Block wird trotzdem für Bildsuche etc. behalten
        
        # --- PRÜFUNG: Leere Frage überspringen ---
        is_question_empty = not question_data["question"] or question_data["question"].isspace()
        are_options_empty = all(
            (not question_data.get(f"option_{l}") or question_data.get(f"option_{l}").isspace()) 
            for l in "abcde"
        )
        
        if is_question_empty and are_options_empty:
            logger.warning(f"Block {block_idx+1} (Nummer {question_data['question_number']}) wird übersprungen, da Frage und Optionen leer sind.")
            continue # Zum nächsten Block gehen
        
        # --- Ende: NEUE LOGIK ---
        
        # --- Entferne alte Options/Meta-Extraktion (ist jetzt oben integriert) ---
        # for letter in "ABCDE":
        # ... (alter Code entfernt) ...
        # fach_match = ... 
        # antwort_match = ...
        # kommentar_match = ...
        
        # --- Beginn: Bildsuche und -zuordnung innerhalb des Blocks (Unverändert) ---
        found_image_in_block = False
        block_page_indices = set() # Seiten, die Text dieses Blocks enthalten

        # 1. Finde heraus, welche Seiten den Text dieses Blocks enthalten
        relevant_pages = []
        for p_info in page_texts:
             # Prüfe auf Überlappung der Zeichenindizes
             if max(block_start_index, p_info["index"]) < min(block_end_index, p_info["index"] + len(p_info["text"])):
                 block_page_indices.add(p_info["page_idx"])
                 relevant_pages.append(p_info["page_idx"])
        
        # Wenn Seiten gefunden wurden, setze die erste als Hauptseite der Frage
        if relevant_pages:
            question_data["page"] = min(relevant_pages)
            # Versuche Y-Position der Frage auf der Seite zu finden (Approximation)
            try:
                 page_text_for_y = doc[question_data["page"]].get_text("text")
                 # Suche nach Anfang der Frage oder ersten Zeile des Blocks
                 search_term = question_data["question"][:30] if question_data["question"] else block_text.split('\n')[0][:30]
                 pos = page_text_for_y.find(search_term)
                 if pos != -1:
                     # Schätze Y basierend auf Zeichenposition (sehr ungenau)
                     page_height = doc[question_data["page"]].rect.height
                     estimated_y = (pos / len(page_text_for_y)) * page_height if len(page_text_for_y) > 0 else 0
                     question_data["y"] = estimated_y
                     #logger.info(f"Geschätzte Y-Position für Frage {question_data['question_number']} auf Seite {question_data['page']}: {estimated_y:.0f}")
            except Exception as e_y:
                 logger.warning(f"Fehler bei Schätzung der Y-Position für Frage {question_data['question_number']}: {e_y}")

        # 2. Suche Bilder auf diesen Seiten und prüfe Zugehörigkeit zum Block
        if relevant_pages:
             for page_idx in sorted(list(relevant_pages)): # Gehe Seiten in Reihenfolge durch
                 try:
                     page = doc[page_idx]
                     # Finde die Bounding Box des Textes, der *auf dieser Seite* zum Block gehört
                     # Verwende page.get_text("blocks"), um Textblöcke mit Koordinaten zu erhalten
                     page_block_rect = fitz.Rect() # BBox des Block-Texts auf dieser Seite
                     page_text_blocks = page.get_text("blocks")
                     
                     # Finde den Start- und End-Index des Block-Texts *relativ* zum Seitenanfang
                     page_start_in_full = page_texts[page_idx]["index"]
                     rel_block_start = max(0, block_start_index - page_start_in_full)
                     rel_block_end = min(len(page_texts[page_idx]["text"]), block_end_index - page_start_in_full)

                     block_text_on_page = page_texts[page_idx]["text"][rel_block_start:rel_block_end]

                     # Approximiere die BBox durch die Blöcke, die Teile des Texts enthalten (ungenau)
                     # Bessere Methode: page.search_for, wenn der Text kurz genug ist.
                     # Hier vereinfachte Annahme: Nutze die y-Koordinate der Frage als Anker
                     
                     # Einfachere Heuristik: Prüfe Bilder auf der Seite
                     page_images = page.get_images(full=True)
                     for img_info in page_images:
                         xref = img_info[0]
                         if xref in extracted_xrefs_global: continue # Schon global verarbeitet

                         img_rect = page.get_image_bbox(img_info)
                         if img_rect.is_empty: continue
                         
                         # Zugehörigkeitsprüfung: Liegt das Bild *nach* der geschätzten Y-Position der Frage
                         # und *vor* der geschätzten Y-Position der *nächsten* Frage (oder Seitenende)?
                         # Dies ist eine Annäherung an die Block-Zugehörigkeit.
                         
                         # Bedingung: Bild muss auf einer Seite des Blocks sein
                         if page_idx in block_page_indices:
                             # Prüfe, ob schon ein Bild zugeordnet wurde
                             if not question_data["image_key"]: 
                                 base_image = doc.extract_image(xref)
                                 if base_image and len(base_image["image"]) > 100:
                                     extracted_xrefs_global.add(xref) # Zum globalen Set hinzufügen
                                     question_data["image_bytes"] = base_image["image"]
                                     question_data["image_ext"] = base_image["ext"]
                                     img_y_coord = int(img_rect.y0) # Verwende y0 als Teil des Schlüssels
                                     question_data["image_key"] = f"{question_data['id']}_{page_idx}_{img_y_coord}.{base_image['ext']}"
                                     # Update page/y basierend auf Bildposition
                                     question_data["page"] = page_idx 
                                     question_data["y"] = img_rect.y0 
                                     logger.info(f"Bild (xref {xref}) auf Seite {page_idx+1} Frage {question_data['question_number']} im Block {block_idx+1} zugeordnet (Erstes Bild im Block). Key: {question_data['image_key']}")
                                     found_image_in_block = True
                                     break # Nur das erste Bild pro Block nehmen
                 except Exception as e_img_search:
                     logger.error(f"Fehler bei Bildsuche in Block {block_idx+1}, Seite {page_idx+1}: {e_img_search}")
                 
                 if found_image_in_block:
                     break # Stoppe Seitensuche für diesen Block, wenn Bild gefunden wurde

        questions.append(question_data)
        
    try:
        doc.close() # Dokument schließen
    except Exception as e_close:
        logger.warning(f"Fehler beim Schließen des PDF-Dokuments: {e_close}")
    
    logger.info(f"Insgesamt {len(questions)} Fragen extrahiert, {sum(1 for q in questions if q['image_key'])} davon mit direkt zugeordneten Bildern.")
    return questions

def parse_question_details(question):
    """
    Parst zusätzliche Details, falls diese in der Extraktion noch nicht erfasst wurden
    """
    # Die meisten Details sollten bereits erfasst sein, aber für den Fall, dass etwas fehlt
    if question.get("option_a") is not None and question.get("option_b") is not None:
        # Frage wurde bereits vollständig geparst
        return question
    
    try:
        full_text = question.get("full_text", "")
        
        # Extrahiere Optionen, falls noch nicht geschehen
        options = {}
        option_matches = re.finditer(r'([A-E])\)(.*?)(?=\s*[A-E]\)|Fach:|Antwort:|Kommentar:|$)', full_text, re.DOTALL)
        for match in option_matches:
            options[match.group(1)] = match.group(2).strip()
        
        # Aktualisiere die Frage mit fehlenden Optionen
        for letter in "ABCDE":
            if letter in options and not question.get(f"option_{letter.lower()}"):
                question[f"option_{letter.lower()}"] = options[letter]
        
        # Extrahiere weitere Metadaten, falls noch nicht geschehen
        if not question.get("subject"):
            fach_match = re.search(r'Fach:\s*(.*?)(?=Antwort:|Kommentar:|$)', full_text, re.DOTALL)
            if fach_match:
                question["subject"] = fach_match.group(1).strip()
        
        if not question.get("correct_answer"):
            answer_match = re.search(r'Antwort:\s*(.*?)(?=Fach:|Kommentar:|$)', full_text, re.DOTALL)
            if answer_match:
                question["correct_answer"] = answer_match.group(1).strip()
        
        if not question.get("comment"):
            comment_match = re.search(r'Kommentar:\s*(.*?)(?=Fach:|Antwort:|$)', full_text, re.DOTALL)
            if comment_match:
                question["comment"] = comment_match.group(1).strip()
        
        return question
        
    except Exception as e:
        logger.error(f"Fehler beim Parsen der Fragedetails: {str(e)}")
        return question

def extract_images_with_coords(pdf_path):
    """
    Optimierte Bildextraktionsfunktion mit Performance-Verbesserungen
    """
    doc = fitz.open(pdf_path)
    images = []
    
    # Cache für extrahierte Bilder, um Duplikate zu vermeiden
    extracted_xrefs = set()
    
    logger.info(f"Starte optimierte Bildextraktion aus PDF mit {len(doc)} Seiten")
    
    # Methode 1: Direkte Bildextraktion über Blöcke (optimiert)
    try:
        # Verwende eine effizientere Schleife mit früherem Abbruch bei Fehlern
        for page_number in range(len(doc)):
            page = doc[page_number]
            logger.info(f"Verarbeite Seite {page_number+1} für Bildextraktion (Methode 1)")
            
            # Hole alle Bildblöcke in einem einzigen Aufruf
            try:
                img_list = page.get_images(full=True)
                logger.info(f"Gefunden: {len(img_list)} Bilder auf Seite {page_number+1}")
            except Exception as page_error:
                logger.error(f"Fehler beim Abrufen der Bilder auf Seite {page_number+1}: {str(page_error)}")
                continue  # Fahre mit der nächsten Seite fort
            
            # Extrahiere alle Bilder dieser Seite in einem Batch
            for img_idx, img_info in enumerate(img_list):
                try:
                    xref = img_info[0]  # Bild-Referenz
                    
                    # Überspringe bereits extrahierte Bilder (Deduplizierung)
                    if xref in extracted_xrefs:
                        logger.info(f"Bild mit xref {xref} bereits extrahiert, überspringe...")
                        continue
                        
                    extracted_xrefs.add(xref)
                    base_image = doc.extract_image(xref)
                    
                    if not base_image:
                        logger.warning(f"Leeres Bild für xref {xref} auf Seite {page_number+1}")
                        continue
                        
                    # Normalisiere die Bounding Box zur Liste [x, y, width, height]
                    bbox = [0, 0, 0, 0]  # Standardwert
                    
                    # Effizientere Positionsbestimmung
                    if len(img_info) > 3:
                        raw_bbox = img_info[3]
                        # Stelle sicher, dass bbox das richtige Format hat
                        if isinstance(raw_bbox, (list, tuple)) and len(raw_bbox) >= 4:
                            bbox = list(raw_bbox)  # Konvertiere zu Liste für Konsistenz
                        elif isinstance(raw_bbox, int):
                            # Wenn es ein Integer ist, verwenden wir eine künstliche Box mit y = raw_bbox
                            bbox = [0, raw_bbox, 100, raw_bbox + 100]
                        else:
                            # Fallback: Verwende Position basierend auf Bildindex
                            bbox = [0, img_idx * 100, 100, (img_idx + 1) * 100]
                    else:
                        # Fallback: Verwende Position basierend auf Bildindex
                        bbox = [0, img_idx * 100, 100, (img_idx + 1) * 100]
                    
                    # Extrahiere nur die benötigten Daten
                    image_bytes = base_image["image"]
                    image_ext = base_image["ext"]
                    
                    # Überprüfe die Bildqualität (überspringe zu kleine Bilder)
                    if len(image_bytes) < 100:
                        logger.warning(f"Bild auf Seite {page_number+1} zu klein ({len(image_bytes)} Bytes), überspringe...")
                        continue
                    
                    images.append({
                        "page": page_number,
                        "bbox": bbox,
                        "image_bytes": image_bytes,
                        "image_ext": image_ext,
                        "question_id": None
                    })
                    logger.info(f"Bild {img_idx+1} von Seite {page_number+1} erfolgreich extrahiert: {image_ext} Format, {len(image_bytes)} Bytes")
                except Exception as img_error:
                    logger.error(f"Fehler bei Bildextraktion (Bild {img_idx+1}, Seite {page_number+1}): {str(img_error)}")
    except Exception as e:
        logger.error(f"Fehler bei Methode 1 der Bildextraktion: {str(e)}")
    
    # Methode 2: Alternative Extraktion (falls Methode 1 keine Bilder findet)
    if len(images) == 0:
        logger.warning("Keine Bilder mit Methode 1 gefunden. Versuche alternative Extraktionsmethode...")
        try:
            for page_number in range(len(doc)):
                page = doc[page_number]
                logger.info(f"Verarbeite Seite {page_number+1} für Bildextraktion (Methode 2)")
                
                # Effizienteres Holen aller Blöcke mit Fehlerbehandlung
                try:
                    page_dict = page.get_text("dict")
                    blocks = page_dict.get("blocks", [])
                except Exception as page_error:
                    logger.error(f"Fehler beim Abrufen der Blöcke auf Seite {page_number+1}: {str(page_error)}")
                    continue
                
                for block_idx, block in enumerate(blocks):
                    if block.get("type") == 1:  # Bildblock
                        try:
                            # Diverses PyMuPDF-Versionshandling
                            xref = None
                            if isinstance(block.get("image"), dict):
                                xref = block["image"].get("xref")
                            elif hasattr(block, "xref"):
                                xref = block.xref
                            
                            # Überspringe bereits extrahierte Bilder
                            if xref in extracted_xrefs:
                                logger.info(f"Bild mit xref {xref} bereits extrahiert (Methode 2), überspringe...")
                                continue
                                
                            if xref:
                                extracted_xrefs.add(xref)
                                base_image = doc.extract_image(xref)
                                
                                if not base_image:
                                    continue
                                    
                                image_bytes = base_image["image"]
                                image_ext = base_image["ext"]
                                
                                # Überprüfe die Bildqualität
                                if len(image_bytes) < 100:
                                    logger.warning(f"Bild auf Seite {page_number+1} zu klein ({len(image_bytes)} Bytes), überspringe...")
                                    continue
                                
                                # Normalisiere die Bounding Box
                                bbox = [0, 0, 0, 0]  # Standardwert
                                if "bbox" in block and isinstance(block["bbox"], (list, tuple)) and len(block["bbox"]) >= 4:
                                    bbox = list(block["bbox"])  # Konvertiere zu Liste für Konsistenz
                                else:
                                    # Fallback: Verwende Position basierend auf Blockindex
                                    bbox = [0, block_idx * 100, 100, (block_idx + 1) * 100]
                                
                                images.append({
                                    "page": page_number,
                                    "bbox": bbox,
                                    "image_bytes": image_bytes,
                                    "image_ext": image_ext,
                                    "question_id": None
                                })
                                logger.info(f"Block-Bild {block_idx+1} von Seite {page_number+1} extrahiert: {image_ext} Format, {len(image_bytes)} Bytes")
                        except Exception as block_error:
                            logger.error(f"Fehler bei Block-Bildextraktion (Block {block_idx+1}, Seite {page_number+1}): {str(block_error)}")
        except Exception as e:
            logger.error(f"Fehler bei Methode 2 der Bildextraktion: {str(e)}")
    
    # Entferne potenzielle Duplikate (basierend auf Bild-Hash)
    unique_images = []
    image_hashes = set()
    
    for img in images:
        # Berechne einen einfachen Hash der ersten 1000 Bytes des Bildes
        if len(img["image_bytes"]) > 0:
            img_hash = hashlib.md5(img["image_bytes"][:1000]).hexdigest()
            
            if img_hash not in image_hashes:
                image_hashes.add(img_hash)
                unique_images.append(img)
    
    if len(unique_images) < len(images):
        logger.info(f"Duplikaterkennung: Von {len(images)} auf {len(unique_images)} Bilder reduziert")
        images = unique_images
    
    # Zusammenfassung
    logger.info(f"Bildextraktion abgeschlossen: {len(images)} einzigartige Bilder aus {len(doc)} Seiten")
    
    # Gib Speicher frei
    doc.close()
    
    # Prüfe Bildqualität und Größe
    if images:
        logger.info(f"Beispiel-Bildgröße: {len(images[0]['image_bytes'])} Bytes, Format: {images[0]['image_ext']}")
    
    return images

def map_images_to_questions(questions, images):
    """
    Optimierte Algorithmus zur Zuordnung von Bildern zu Fragen mit verbesserter Performance
    """
    logger.info(f"Starte optimierte Bildzuordnung: {len(images)} Bilder zu {len(questions)} Fragen")
    
    if not images or not questions:
        logger.warning("Keine Bilder oder Fragen vorhanden für die Zuordnung")
        return images
    
    try:
        # Organisiere Fragen nach Seiten in einer effizienten Datenstruktur
        # Verwende vorberechnete Sortierung für schnelleren Zugriff
        questions_by_page = {}
        for q in questions:
            page = q.get("page", -1)
            if page >= 0:  # Ignoriere Fragen ohne Seitenzuordnung
                if page not in questions_by_page:
                    questions_by_page[page] = []
                questions_by_page[page].append(q)
        
        # Vorsortiere Fragen nach Y-Koordinate innerhalb jeder Seite (steigert die Performance)
        for page, page_questions in questions_by_page.items():
            # Sortiere Fragen nach Y-Koordinate (aufsteigend)
            page_questions.sort(key=lambda q: float(q.get("y", 0)) if isinstance(q.get("y", 0), (int, float, str)) else 0)
        
        # Verarbeite alle Bilder in einem effizienten Durchlauf
        start_time = datetime.now()
        assigned_count = 0
        
        # Gruppiere Bilder nach Seiten für effizientere Verarbeitung
        images_by_page = {}
        for img_idx, img in enumerate(images):
            if not isinstance(img, dict):
                logger.error(f"Bild {img_idx} ist kein Dictionary: {type(img)}")
                continue
                
            page = img.get("page", -1)
            # Konvertiere page zu int, falls es ein string ist
            if isinstance(page, str) and page.isdigit():
                page = int(page)
                
            if page not in images_by_page:
                images_by_page[page] = []
            images_by_page[page].append((img_idx, img))
        
        # Verarbeite alle Bilder seitenweise
        for page, page_images in images_by_page.items():
            # Überprüfe, ob es Fragen auf dieser Seite gibt
            if page not in questions_by_page:
                logger.info(f"Keine Fragen auf Seite {page} für {len(page_images)} Bilder")
                continue
            
            page_questions = questions_by_page[page]
            
            # Sortiere Bilder nach Y-Koordinate für effizientere Zuordnung
            def get_image_y(img_tuple):
                _, img = img_tuple
                bbox = img.get("bbox", [0, 0, 0, 0])
                
                if isinstance(bbox, (list, tuple)) and len(bbox) > 1:
                    y = bbox[1]
                elif isinstance(bbox, int):
                    y = bbox
                else:
                    y = 0
                    
                # Stelle sicher, dass y numerisch ist
                if not isinstance(y, (int, float)):
                    try:
                        y = float(y)
                    except (ValueError, TypeError):
                        y = 0
                        
                return y
                
            # Sortiere nach Y-Koordinate für effizientere Verarbeitung
            page_images.sort(key=get_image_y)
            
            # Optimierte Zuordnung mit Binary Search Annäherung
            for img_idx, img in page_images:
                # Extrahiere Y-Koordinate des Bildes
                bbox = img.get("bbox", [0, 0, 0, 0])
                img_y = 0
                
                if isinstance(bbox, (list, tuple)) and len(bbox) > 1:
                    img_y = bbox[1]
                elif isinstance(bbox, int):
                    img_y = bbox
                
                # Stelle sicher, dass img_y numerisch ist
                if not isinstance(img_y, (int, float)):
                    try:
                        img_y = float(img_y)
                    except (ValueError, TypeError):
                        img_y = 0
                
                # Optimierte Kandidatensuche mit Linear Scan (schneller für sortierte Listen)
                # Binary Search wäre hier zu komplex für den potenziellen Gewinn
                best_question = None
                best_distance = float('inf')
                
                for q in page_questions:
                    q_y = q.get("y", 0)
                    # Stelle sicher, dass q_y numerisch ist
                    if not isinstance(q_y, (int, float)):
                        try:
                            q_y = float(q_y)
                        except (ValueError, TypeError):
                            q_y = 0
                    
                    # Berechne Distanz - bevorzuge Fragen oberhalb des Bildes
                    if q_y <= img_y:  # Frage ist oberhalb des Bildes
                        distance = img_y - q_y
                    else:
                        # Frage ist unterhalb - weniger bevorzugt, aber möglich
                        distance = (q_y - img_y) * 2  # Gewichtungsfaktor
                    
                    if distance < best_distance:
                        best_distance = distance
                        best_question = q
                
                # Wenn ein passender Kandidat gefunden wurde
                if best_question:
                    # Weise das Bild der Frage zu
                    img["question_id"] = best_question.get("id")
                    
                    # Erstelle einen Bildschlüssel
                    image_key = f"{best_question.get('id')}_{page}_{int(float(img_y))}.{img.get('image_ext', 'jpg')}"
                    
                    # Aktualisiere die Frage mit dem Bildschlüssel
                    for q in questions:
                        if q.get("id") == best_question.get("id"):
                            q["image_key"] = image_key
                            logger.info(f"Bild {img_idx} zugeordnet zu Frage {q.get('question_number', '?')} auf Seite {page}, Distanz: {best_distance:.2f}")
                            assigned_count += 1
                            break
                elif page_questions:  # Fallback: Verwende die erste Frage auf der Seite, wenn keine passende gefunden wurde
                    first_question = page_questions[0]
                    img["question_id"] = first_question.get("id")
                    image_key = f"{first_question.get('id')}_{page}_{int(float(img_y))}.{img.get('image_ext', 'jpg')}"
                    
                    for q in questions:
                        if q.get("id") == first_question.get("id"):
                            q["image_key"] = image_key
                            logger.info(f"Bild {img_idx} der ersten Frage auf Seite {page} zugeordnet (Fallback)")
                            assigned_count += 1
                            break
        
        # Statistiken zur Leistungsmessung
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        logger.info(f"Bildzuordnung abgeschlossen: {assigned_count} von {len(images)} Bildern zugeordnet, Dauer: {processing_time:.2f}s")
        
        return images
    
    except Exception as e:
        logger.error(f"Fehler bei der optimierten Bildzuordnung: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return images

def insert_questions_into_db(questions, exam_name, exam_year, exam_semester, user_id, visibility, config):
    """
    Fügt Fragen in Supabase im Bulk-Modus ein für bessere Performance
    """
    global current_pdf_filename
    supabase = config.supabase
    successful = 0
    failed = 0
    
    logger.info(f"Füge {len(questions)} Fragen in Supabase ein, Datei: {current_pdf_filename}")
    
    # Verwende den globalen Dateinamen oder einen Fallback
    pdf_filename = current_pdf_filename
    if not pdf_filename:
        # Fallback-Dateiname generieren (sollte selten vorkommen)
        pdf_filename = f"{exam_name}_{exam_year}_{exam_semester}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        logger.warning(f"Kein Dateiname vorhanden, verwende generierten Namen: {pdf_filename}")
    
    # Bereite Daten für Bulk-Upload vor
    bulk_data = []
    for q in questions:
        # Die Struktur kommt jetzt von formatted_questions, parse_question_details ist hier nicht mehr nötig
        # (wurde bereits in process_pdf angewendet)
            
        # Sicherstellen, dass alle Werte Strings sind
        options_dict = q.get("options", {}) # Hole das verschachtelte Optionen-Dict

        data = {
            "id": str(q.get("id", uuid.uuid4())),
            "exam_name": str(exam_name or ""),
            "exam_year": str(exam_year or ""),
            "exam_semester": str(exam_semester or ""),
            "question": str(q.get("question", "")),
            "option_a": str(options_dict.get("A", "")), # Zugriff über options_dict
            "option_b": str(options_dict.get("B", "")), # Zugriff über options_dict
            "option_c": str(options_dict.get("C", "")), # Zugriff über options_dict
            "option_d": str(options_dict.get("D", "")), # Zugriff über options_dict
            "option_e": str(options_dict.get("E", "")), # Zugriff über options_dict
            "subject": str(q.get("subject", "")),
            "correct_answer": str(q.get("correctAnswer", "")), # Beachte: Key ist hier correctAnswer
            "comment": str(q.get("comment", "")),
            "image_key": str(q.get("image_key", "")),
            "filename": pdf_filename,
            "user_id": str(user_id) if user_id else None, # Add user_id
            "visibility": str(visibility) # Add visibility
        }
        bulk_data.append(data)
    
    # Falls keine Daten vorhanden sind, beende frühzeitig
    if not bulk_data:
        logger.warning("Keine Fragen zum Einfügen vorhanden")
        return 0, 0
    
    # Performance-Optimierung: Batch-Upload in Gruppen von 50
    batch_size = 50
    
    try:
        # Verarbeite die Daten in Batches
        for i in range(0, len(bulk_data), batch_size):
            batch = bulk_data[i:i+batch_size]
            logger.info(f"Verarbeite Batch {i//batch_size + 1}/{(len(bulk_data) + batch_size - 1)//batch_size}: {len(batch)} Fragen")
            
            try:
                response = supabase.table('questions').upsert(batch).execute()
                
                # Prüfe auf Fehler im Response
                if hasattr(response, 'error') and response.error:
                    logger.error(f"Fehler beim Batch-Upload (Batch {i//batch_size + 1}): {response.error}")
                    failed += len(batch)
                else:
                    # Zähle erfolgreiche Datensätze
                    if hasattr(response, 'data') and response.data:
                        successful += len(response.data)
                    else:
                        successful += len(batch)  # Annahme: alle erfolgreich, wenn kein expliziter Fehler
                        
                logger.info(f"Batch {i//batch_size + 1} abgeschlossen: {len(batch)} Datensätze verarbeitet")
                
            except Exception as batch_error:
                logger.error(f"Fehler bei Batch {i//batch_size + 1}: {str(batch_error)}")
                failed += len(batch)
                
                # Notfallverarbeitung: Versuche einzeln zu verarbeiten, wenn Batch fehlschlägt
                logger.warning(f"Versuche Einzelverarbeitung für Batch {i//batch_size + 1}")
                for idx, item in enumerate(batch):
                    try:
                        single_response = supabase.table('questions').upsert(item).execute()
                        if not (hasattr(single_response, 'error') and single_response.error):
                            successful += 1
                            failed -= 1  # Korrigiere den früher gezählten Fehler
                        logger.info(f"Einzelverarbeitung für Item {idx+1}/{len(batch)} erfolgreich")
                    except Exception as e:
                        logger.error(f"Einzelverarbeitung für Item {idx+1}/{len(batch)} fehlgeschlagen: {str(e)}")
    
    except Exception as e:
        logger.error(f"Allgemeiner Fehler beim Datenbank-Upload: {str(e)}")
        # Zähle alle verbleibenden als fehlgeschlagen
        remaining = len(bulk_data) - (successful + failed)
        if remaining > 0:
            failed += remaining
    
    logger.info(f"Datenbankvorgang abgeschlossen: {successful} erfolgreich, {failed} fehlgeschlagen")
    return successful, failed

def upload_image_to_s3(image_bytes, filename, bucket_name, s3_config):
    """
    Lädt ein Bild zuverlässig in MinIO hoch
    """
    if not image_bytes:
        logger.error(f"Keine Bilddaten zum Hochladen für {filename}")
        return False
    
    try:
        logger.info(f"Lade Bild {filename} ({len(image_bytes)} Bytes) in Bucket {bucket_name} hoch")
        
        # Erstelle S3-Client mit detaillierter Konfiguration
        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_config["endpoint_url"],
            aws_access_key_id=s3_config["aws_access_key_id"],
            aws_secret_access_key=s3_config["aws_secret_access_key"],
            config=boto3.session.Config(signature_version='s3v4')
        )
        
        # Prüfe, ob der Bucket existiert, sonst erstelle ihn
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            logger.info(f"Bucket {bucket_name} nicht gefunden, versuche zu erstellen")
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Bucket {bucket_name} erfolgreich erstellt")
            except Exception as bucket_error:
                logger.error(f"Fehler beim Erstellen des Buckets {bucket_name}: {str(bucket_error)}")
        
        # Lade das Bild hoch
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=image_bytes,
            ContentType=f'image/{filename.split(".")[-1].lower()}'
        )
        
        logger.info(f"Bild {filename} erfolgreich hochgeladen")
        return True
    
    except Exception as e:
        logger.error(f"Fehler beim Hochladen des Bildes {filename}: {str(e)}")
        return False

def upload_image_to_supabase(image_bytes, filename, bucket_name, config):
    """
    Lädt ein Bild zuverlässig in Supabase Storage hoch
    """
    if not image_bytes:
        logger.error(f"Keine Bilddaten zum Hochladen für {filename}")
        return False
    
    try:
        logger.info(f"Lade Bild {filename} ({len(image_bytes)} Bytes) in Supabase Storage Bucket {bucket_name} hoch")
        
        # Bestimme den Content-Type basierend auf der Dateiendung
        file_extension = filename.split(".")[-1].lower()
        content_type = f'image/{file_extension}'
        
        # Upload-Optionen
        options = {
            'content-type': content_type,
            'upsert': 'true'  # Überschreibe falls die Datei existiert
        }
        
        # Führe den Upload mit Supabase Storage durch
        response = config.supabase.storage.from_(bucket_name).upload(filename, image_bytes, options)
        
        # Prüfe auf Fehler in der Antwort
        if hasattr(response, 'error') and response.error:
            logger.error(f"Supabase Storage Upload-Fehler: {response.error}")
            return False
            
        logger.info(f"Bild {filename} erfolgreich in Supabase Storage hochgeladen")
        return True
    
    except Exception as e:
        logger.error(f"Fehler beim Hochladen des Bildes nach Supabase Storage {filename}: {str(e)}")
        return False

def update_question_image_key(question_id, image_key, db_config):
    """
    Aktualisiert den image_key für eine Frage in der Datenbank.
    """
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    update_sql = "UPDATE exam_questions SET image_key = %s WHERE id = %s"
    cur.execute(update_sql, (image_key, question_id))
    conn.commit()
    cur.close()
    conn.close()

def main(pdf_path):
    try:
        config = Config()
        
        # Prüfungsheader extrahieren
        exam_name, exam_year, exam_semester = extract_exam_header(pdf_path)
        print("Prüfungsdaten:", exam_name, exam_year, exam_semester)

        # Fragen extrahieren und verarbeiten
        questions = extract_questions_with_coords(pdf_path)
        for q in questions:
            parse_question_details(q)
        print(f"{len(questions)} Fragen extrahiert.")

        # Bilder verarbeiten (MinIO beibehalten)
        images = extract_images_with_coords(pdf_path)
        images = map_images_to_questions(questions, images)
        print(f"{len(images)} Bilder extrahiert.")

        bucket_name = "exam-images"
        for img in images:
            if img.get("question_id"):
                filename = f"{img['question_id']}_{img['page']}_{int(img['bbox'][1])}.{img['image_ext']}"
                # Supabase Storage Upload
                upload_image_to_supabase(
                    img["image_bytes"], 
                    filename, 
                    bucket_name, 
                    config
                )
                # Aktualisiere die zugehörige Frage
                for q in questions:
                    if q["id"] == img["question_id"]:
                        q["image_key"] = filename
                        break

        # Fragen in Supabase speichern
        insert_questions_into_db(questions, exam_name, exam_year, exam_semester, None, "private", config)
        
        return {
            "status": "success", 
            "message": f"PDF verarbeitet: {len(questions)} Fragen, {len(images)} Bilder"
        }
        
    except Exception as e:
        logger.error(f"Fehler in main: {str(e)}")
        return {"status": "error", "message": str(e)}

def analyze_pdf_structure(pdf_path):
    """Analysiert ein PDF und gibt detaillierte Informationen zur Struktur aus"""
    doc = fitz.open(pdf_path)
    logger.info(f"PDF-Analyse für {pdf_path}: {len(doc)} Seiten")
    
    # Analysiere erste, mittlere und letzte Seite
    sample_pages = [0, len(doc)//2, len(doc)-1]
    for page_idx in sample_pages:
        if 0 <= page_idx < len(doc):
            page = doc[page_idx]
            text = page.get_text()
            logger.info(f"--- Seite {page_idx+1} Textprobe ---")
            logger.info(text[:300] + "..." if len(text) > 300 else text)
            
            # Suche nach bestimmten Mustern
            patterns = {
                "Frage-Muster 1": r"\d+\.\s*Frage:",
                "Frage-Muster 2": r"Frage\s*\d+[:\.]",
                "Optionen": r"[A-E]\)",
                "Unterstrichtrennungen": r"_{5,}",
            }
            
            for name, pattern in patterns.items():
                matches = re.findall(pattern, text)
                logger.info(f"{name}: {len(matches)} Treffer - Beispiele: {matches[:3]}")
    
    return {"pages": len(doc), "analyzed_samples": sample_pages}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        workers=1,
        log_level="info"
    )