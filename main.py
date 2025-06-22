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
import docx
from io import BytesIO

# Logging Konfiguration with a custom format
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

def validate_file(file: UploadFile) -> bool:
    """Validiert die Datei (PDF oder DOCX)"""
    filename = file.filename.lower()
    if not (filename.endswith('.pdf') or filename.endswith('.docx')):
        return False
    # Hier könnten weitere Validierungen hinzugefügt werden
    return True

async def process_document(file_path: str, config: Config, metadata: Dict) -> Dict:
    """Verarbeitet eine hochgeladene Datei (PDF oder DOCX) basierend auf ihrer Endung."""
    file_extension = os.path.splitext(file_path)[1].lower()
    
    if file_extension == ".pdf":
        # Rufe die bestehende PDF-Verarbeitungslogik auf
        return await process_pdf(file_path, config, metadata)
    elif file_extension == ".docx":
        # Rufe die neue DOCX-Verarbeitungslogik auf
        return await process_docx(file_path, config, metadata)
    else:
        # Dieser Fall sollte durch die Validierung bereits abgedeckt sein
        logger.error(f"Nicht unterstützter Dateityp: {file_extension}")
        return {
            "status": "failed",
            "success": False,
            "message": f"Nicht unterstützter Dateityp: {file_extension}",
            "data": {},
            "questions": []
        }

async def process_docx(docx_path: str, config: Config, metadata: Dict) -> Dict:
    """Verarbeitet eine DOCX-Datei, extrahiert Fragen und Bilder."""
    try:
        logger.info(f"Starte DOCX-Verarbeitung für: {docx_path}")
        doc = docx.Document(docx_path)
        
        # Extrahiere Metadaten aus dem Dateinamen als Fallback
        extracted_exam_name, _, _ = extract_exam_header(docx_path)
        exam_name = metadata.get("exam_name") or extracted_exam_name
        exam_year = metadata.get("exam_year", "")
        exam_semester = metadata.get("exam_semester", "")
        default_subject = metadata.get("subject", "")

        logger.info(f"Verarbeite DOCX: {exam_name} {exam_year} {exam_semester}")

        # Extrahiere Fragen und Bilder aus dem DOCX-Dokument
        questions, images = extract_content_from_docx(doc)
        
        if not questions:
            logger.warning("Keine Fragen im DOCX gefunden.")
            return {
                "status": "completed",
                "success": False,
                "message": "Keine Fragen im DOCX gefunden",
                "data": {"exam_name": exam_name, "total_questions_extracted": 0, "total_questions_processed": 0, "questions_ignored": 0, "images_uploaded": 0},
                "questions": []
            }
        
        logger.info(f"{len(questions)} Fragen und {len(images)} Bilder aus DOCX extrahiert.")
        
        # Ordne Bilder den Fragen zu (DOCX-spezifische Logik)
        if images and questions:
            images = map_images_to_questions_docx(questions, images)
            
        bucket_name = "exam-images"
        upload_tasks = []
        processed_image_keys = set()
        
        # Bereite Bild-Uploads vor
        for q in questions:
            if q.get("image_key") and q["image_key"] not in processed_image_keys:
                img_to_upload = next((img for img in images if img.get("image_key") == q["image_key"]), None)
                if img_to_upload:
                    upload_tasks.append({
                        "filename": q["image_key"],
                        "image_bytes": img_to_upload["image_bytes"],
                        "content_type": f'image/{img_to_upload.get("image_ext", "png")}',
                        "question_id": q["id"]
                    })
                    processed_image_keys.add(q["image_key"])

        # Lade Bilder asynchron hoch
        successful_uploads = 0
        try:
            batch_size = 5
            for i in range(0, len(upload_tasks), batch_size):
                batch = upload_tasks[i:i+batch_size]
                upload_futures = []
                for task in batch:
                    upload_future = asyncio.ensure_future(upload_image_async(
                        config, task["image_bytes"], task["filename"], bucket_name, task["content_type"]
                    ))
                    upload_futures.append((task, upload_future))
                
                for task, future in upload_futures:
                    try:
                        if await future:
                            successful_uploads += 1
                            logger.info(f"Bild {task['filename']} erfolgreich hochgeladen (verknüpft mit Frage {task['question_id']})")
                    except Exception as e:
                        logger.error(f"Fehler beim asynchronen Supabase-Upload von {task['filename']}: {str(e)}")
        except Exception as async_error:
            logger.error(f"Fehler bei der asynchronen Supabase-Upload-Methode: {str(async_error)}")

        # Bereite Fragedaten für die Antwort auf
        formatted_questions = []
        ignored_questions_count = 0
        for q in questions:
            if should_ignore_question(q):
                ignored_questions_count += 1
                continue
            
            subject = q.get("subject") or default_subject
            formatted_question = {
                "id": q.get("id", str(uuid.uuid4())),
                "question": q.get("question", ""),
                "options": {
                    "A": q.get("option_a", ""), "B": q.get("option_b", ""),
                    "C": q.get("option_c", ""), "D": q.get("option_d", ""),
                    "E": q.get("option_e", "")
                },
                "correctAnswer": q.get("correct_answer", ""),
                "subject": subject,
                "comment": q.get("comment", ""),
                "difficulty": 3,
                "semester": exam_semester,
                "year": exam_year,
                "image_key": q.get("image_key", "")
            }
            formatted_questions.append(formatted_question)
        
        # Log detailed filtering information
        if ignored_questions_count > 0:
            logger.info(f"DOCX-Verarbeitung: {ignored_questions_count} von {len(questions)} Fragen wurden ignoriert.")
            logger.info(f"Gründe: zu kurzer Text, keine Antwortoptionen, oder nur Platzhalter-Text.")
            
        return {
            "status": "completed",
            "success": True,
            "data": {
                "exam_name": exam_name,
                "images_uploaded": successful_uploads,
                "total_questions_extracted": len(questions),
                "total_questions_processed": len(formatted_questions),
                "questions_ignored": ignored_questions_count,
                "total_images": len(images),
            },
            "questions": formatted_questions
        }

    except Exception as e:
        logger.error(f"Fehler bei der DOCX-Verarbeitung: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "failed",
            "success": False,
            "message": f"DOCX processing error: {str(e)}",
            "data": {},
            "questions": []
        }

async def process_pdf(pdf_path: str, config: Config, metadata: Dict) -> Dict:
    """Verarbeitet das PDF mit verbesserter Fehlerbehandlung und Performance-Optimierungen"""
    doc = None # Initialisiere doc
    try:
        # Öffne das PDF-Dokument einmal am Anfang
        doc = fitz.open(pdf_path)

        # Extrahiere Header aus Dateinamen (als Fallback, wenn keine Metadaten angegeben)
        extracted_exam_name, extracted_exam_year, extracted_exam_semester = extract_exam_header(pdf_path)
        
        # Verwende übergebene Metadaten mit Fallback auf extrahierte Werte
        exam_name = metadata.get("exam_name") or extracted_exam_name
        exam_year = metadata.get("exam_year") or extracted_exam_year
        exam_semester = metadata.get("exam_semester") or extracted_exam_semester
        default_subject = metadata.get("subject", "")
        
        logger.info(f"Verarbeite PDF: {exam_name} {exam_year} {exam_semester}")

        # Extrahiere und verarbeite Fragen
        # Wichtig: Pass doc an extract_questions_with_coords, wenn dort genaue Y gebraucht werden
        questions = extract_questions_with_coords(doc) # Übergibt das doc Objekt
        if not questions:
            logger.warning("Keine Fragen im PDF gefunden")
            return {
                "status": "completed",  # Status ist abgeschlossen, auch wenn nichts gefunden wurde
                "success": False, # Explizit als nicht erfolgreich markieren
                "message": "Keine Fragen im PDF gefunden",
                "data": {
                    "exam_name": exam_name,
                    "total_questions_extracted": 0,
                    "total_questions_processed": 0,
                    "questions_ignored": 0,
                    "images_uploaded": 0,
                },
                "questions": [] # Leere Liste für Konsistenz
            }
            
        # Parse Details (optional, falls extract schon alles macht)
        # ... (Batch-Verarbeitung wie zuvor) ...
        batch_size = 20 # Beispiel Batch-Größe
        for i in range(0, len(questions), batch_size):
            batch = questions[i:i+batch_size]
            for q in batch:
                 parse_question_details(q) # Oder im Batch

        logger.info(f"{len(questions)} Fragen extrahiert und verarbeitet")

        # Verarbeite Bilder mit verbesserter Fehlerbehandlung und Performance
        images = []
        try:
            logger.info("Starte optimierte Bildextraktion")
            # Wichtig: Pass doc an extract_images_with_coords
            images = extract_images_with_coords(doc) # Verwende das geöffnete doc
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
            
            # Ordne Bilder den Fragen zu - *** MIT NEUER FUNKTION UND doc ***
            if images and questions:
                try:
                    # Übergebe das doc-Objekt an die Mapping-Funktion
                    images = map_images_to_questions(questions, images, doc) # doc is not used in the new version, but signature kept for now
                    # REMOVE MISLEADING LOG: The new function doesn't use block-based assignment.
                    # A new, more accurate log will be part of the map_images_to_questions function itself.
                    # logger.info(f"Block-basierte Bildzuordnung: {sum(1 for img in images if img.get('question_id'))} Bilder zugeordnet")
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
                    connections_ok = False
        except Exception as s3_error:
            logger.error(f"Fehler bei S3-Client-Initialisierung: {str(s3_error)}")
            s3_client = None
        
        # Bereite Bildupload-Tasks für Supabase Storage vor
        upload_tasks = []
        processed_image_keys = set() # Um doppelte Uploads zu vermeiden, falls mehrere Bilder denselben Key bekommen

        # Gehe durch die Fragen, um den zugewiesenen image_key zu finden
        for q in questions:
            image_key_on_q = q.get("image_key")
            if not image_key_on_q:
                continue

            found_img_for_upload = None # Renamed for clarity
            for img_candidate in images:
                # Preliminary check: was this image candidate even assigned to the current question q?
                if img_candidate.get("question_id") != q.get("id"):
                    continue

                # Now, reconstruct the key for this img_candidate using the midpoint-Y logic,
                # identical to how it's done in map_images_to_questions.
                img_cand_page = img_candidate.get("page")
                img_cand_bbox = img_candidate.get("bbox")
                img_cand_ext = img_candidate.get("image_ext", "jpg") # Default to jpg

                # Ensure necessary data is present for key reconstruction
                if img_cand_page is None or not (isinstance(img_cand_bbox, (list, tuple)) and len(img_cand_bbox) == 4):
                    logger.debug(f"Image candidate for q {q.get('id')} (key: {image_key_on_q}) lacks page or full bbox. Skipping key reconstruction for this candidate.")
                    continue
                
                # Calculate the y-component of the key using the midpoint
                mid_y_val = (img_cand_bbox[1] + img_cand_bbox[3]) / 2
                key_y_from_img_midpoint = int(mid_y_val)

                # Reconstruct the key
                # The q.get("id") is the correct question ID part, as image_key_on_q was created using it.
                reconstructed_key_for_candidate = f"{q.get('id')}_{img_cand_page}_{key_y_from_img_midpoint}.{img_cand_ext}"

                if image_key_on_q == reconstructed_key_for_candidate:
                    # This img_candidate matches the image_key_on_q
                    if img_candidate.get("image_bytes") and len(img_candidate["image_bytes"]) >= 100:
                        found_img_for_upload = img_candidate
                        break # Found the definitive, valid image for this question's image_key
                    else:
                        logger.warning(f"Image data for key {image_key_on_q} (Question {q.get('question_number', '?')}) found, but image_bytes are invalid or empty. Will not upload.")
                        # We found the metadata match, but data is bad. Stop searching for this q.
                        break 
            
            if found_img_for_upload and image_key_on_q not in processed_image_keys:
                 upload_tasks.append({
                    "filename": image_key_on_q, # Use the original key from the question
                    "image_bytes": found_img_for_upload["image_bytes"],
                    "content_type": f'image/{found_img_for_upload.get("image_ext", "jpg")}',
                    "question_id": q["id"] # For logging
                })
                 processed_image_keys.add(image_key_on_q)
            elif not found_img_for_upload and image_key_on_q: 
                # This warning will now be more accurate, as it means either the image link (question_id) was lost,
                # or the properties (page, bbox, ext) changed, or it was found but had no bytes.
                logger.warning(f"No valid, usable image found for key {image_key_on_q} associated with question {q.get('question_number', '?')} (ID: {q.get('id')}) during upload preparation.")

        # Versuche zuerst den asynchronen Upload mit Supabase
        successful_uploads = 0 # Reset counter before upload loop
        try:
            # Führe parallele Uploads durch (in Batches für Kontrolle)
            batch_size = 5  # Anzahl paralleler Uploads
            for i in range(0, len(upload_tasks), batch_size):
                batch = upload_tasks[i:i+batch_size]
                
                # Verarbeite diesen Batch an Uploads
                upload_futures = []
                
                for task in batch:
                    # Asynchrone Funktion für Supabase Upload
                    upload_future = asyncio.ensure_future(upload_image_async(
                        config, # config Objekt übergeben
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
                            # Image Key ist bereits in 'questions' gesetzt
                            logger.info(f"Bild {task['filename']} erfolgreich hochgeladen (verknüpft mit Frage {task['question_id']})")
                            # break nicht nötig, da Key schon in q ist
                    except Exception as e:
                        logger.error(f"Fehler beim asynchronen Supabase-Upload von {task['filename']}: {str(e)}")
        
        except Exception as async_error:
            logger.error(f"Fehler bei der asynchronen Supabase-Upload-Methode: {str(async_error)}")
            logger.warning("Asynchroner Upload fehlgeschlagen. Synchroner Fallback nicht implementiert in diesem Snippet.")
            # Optional: Füge hier synchronen Fallback hinzu, falls benötigt
            # ...

        # Bereite Fragedaten für das Frontend auf (anstatt sie direkt in die Datenbank einzufügen)
        formatted_questions = []
        ignored_questions_count = 0
        
        for q in questions:
            # Prüfe, ob die Frage ignoriert werden soll
            if should_ignore_question(q):
                ignored_questions_count += 1
                continue
                
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
        
        # Log information about ignored questions
        if ignored_questions_count > 0:
            logger.info(f"{ignored_questions_count} Fragen wurden aufgrund der Filter-Kriterien ignoriert (zu kurzer Text, keine Optionen, etc.)")
        
        logger.info(f"{len(formatted_questions)} Fragen nach Filterung für Upload vorbereitet (von ursprünglich {len(questions)} extrahierten Fragen)")

        return {
            "status": "completed", # Konsistent "completed" verwenden
            "success": True,  # Für Frontend-Kompatibilität
            "data": {
                "exam_name": exam_name,
                "images_uploaded": successful_uploads,
                "total_questions_extracted": len(questions),
                "total_questions_processed": len(formatted_questions),
                "questions_ignored": ignored_questions_count,
                "total_images": len(images),
            },
            "questions": formatted_questions  # Neue Struktur für das Frontend
        }

    except Exception as e:
        logger.error(f"Fehler bei der PDF-Verarbeitung: {str(e)}")
        import traceback # Importiere traceback hier
        logger.error(traceback.format_exc()) # Mehr Details loggen
        return {
            "status": "failed", # Konsistent "failed" verwenden
            "success": False,  # Für Frontend-Kompatibilität
            "message": f"PDF processing error: {str(e)}", # Etwas generischer
            "data": {},
            "questions": []
        }
    finally:
        # Stelle sicher, dass das Dokument geschlossen wird
        if doc:
            doc.close()
            logger.info("PDF-Dokument geschlossen.")

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
    summary="Verarbeitet eine PDF- oder DOCX-Datei",
    response_description="Verarbeitungsstatus und Details")
async def upload_document(
    file: UploadFile = File(...),
    examName: str = Form(""),
    examYear: str = Form(""),
    examSemester: str = Form(""),
    subject: str = Form(""),
    userId: str = Form(""), # Receive userId
    visibility: str = Form("private"), # Receive visibility, default to private
    university_id: str = Form(""), # Receive university_id for university visibility
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
    
    # Validiere visibility
    if visibility not in ["private", "university"]:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "error",
                "success": False,
                "message": "Visibility muss entweder 'private' oder 'university' sein",
                "data": {}
            }
        )
    
    # Validiere university_id wenn visibility = "university"
    validated_university_id = None
    if visibility == "university":
        if not university_id:
            logger.warning(f"University visibility requested but no university_id provided. Falling back to private visibility.")
            visibility = "private"
        else:
            # Validiere UUID format
            try:
                import uuid as uuid_module
                uuid_module.UUID(university_id)
                validated_university_id = university_id
                logger.info(f"University assignment: Questions will be assigned to university {university_id}")
            except ValueError:
                logger.warning(f"Invalid university_id format: {university_id}. Falling back to private visibility.")
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={
                        "status": "error",
                        "success": False,
                        "message": "University_id muss ein gültiges UUID-Format haben",
                        "data": {}
                    }
                )
    elif university_id:
        logger.info(f"University_id provided ({university_id}) but visibility is private. University_id will be ignored.")
    
    try:
        if not validate_file(file):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Ungültige PDF- oder DOCX-Datei"
            )

        # Verbesserte temporäre Dateibehandlung
        temp_dir = tempfile.gettempdir()
        file_extension = os.path.splitext(file.filename)[1].lower()
        temp_file_path = os.path.join(temp_dir, f"upload_{uuid.uuid4()}{file_extension}")
        
        logger.info(f"Speichere Datei '{current_pdf_filename}' in temporäre Datei: {temp_file_path}")
        
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
            "visibility": visibility, # Pass visibility to metadata
            "university_id": validated_university_id # Pass validated university_id to metadata
        }
        
        # Erstelle eine Task-ID für das Tracking
        task_id = str(uuid.uuid4())
        
        # Initialisiere den Task-Status
        processing_tasks[task_id] = {
            "status": "processing",
            "message": "PDF-Verarbeitung gestartet",
            "data": {
                "exam_name": examName,
                "filename": file.filename,
                "visibility": visibility,
                "university_id": validated_university_id
            }
        }
        
        # Im asynchronen Modus starten wir die Verarbeitung im Hintergrund
        # und geben sofort eine Antwort zurück
        background_tasks.add_task(
            process_document_in_background, 
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

async def process_document_in_background(task_id: str, pdf_path: str, config: Config, metadata: Dict):
    """
    Verarbeitet die Datei im Hintergrund und aktualisiert den Task-Status.
    """
    try:
        global processing_tasks # Declare processing_tasks as global
        logger.info(f"Starte Hintergrundverarbeitung für Task {task_id}: {pdf_path}")
        
        # 1. Verarbeite die Datei (Extraktion, Bild-Upload)
        processing_result = await process_document(pdf_path, config, metadata)
        
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
                    university_id = metadata.get("university_id", None) # Get university_id from metadata

                    successful_inserts, failed_inserts = insert_questions_into_db(
                        formatted_questions, exam_name, exam_year, exam_semester, user_id, visibility, university_id, config
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

def extract_questions_with_coords(pdf_path_or_doc): # Akzeptiert Pfad oder Doc
    """
    Optimierte Extraktion von Fragen speziell für Altfragen-Format mit Unterstrichtrennlinien
    """
    doc = None
    close_doc_at_end = False # Flag, ob wir das Dokument hier schließen müssen
    if isinstance(pdf_path_or_doc, str):
        logger.info(f"Extrahiere Fragen aus PDF-Pfad: {pdf_path_or_doc}")
        try:
            doc = fitz.open(pdf_path_or_doc)
            close_doc_at_end = True
        except Exception as e:
             logger.error(f"Konnte PDF nicht öffnen: {pdf_path_or_doc} - {str(e)}")
             return [] # Leere Liste bei Fehler
    elif isinstance(pdf_path_or_doc, fitz.Document):
        logger.info(f"Extrahiere Fragen aus bereits geöffnetem PDF-Dokument.")
        doc = pdf_path_or_doc
        close_doc_at_end = False # Nicht hier schließen
    else:
        logger.error("Ungültiges Argument für extract_questions_with_coords. Erwartet Pfad oder fitz.Document.")
        raise ValueError("Ungültiges Argument für extract_questions_with_coords.")

    questions = []
    try:
        logger.info(f"PDF hat {len(doc)} Seiten")

        # Füge den gesamten Text zusammen
        full_text = ""
        for page in doc:
            page_text = page.get_text()
            full_text += page_text
        
        # Debug-Ausgabe eines Textausschnitts
        logger.info(f"Textprobe (erste 300 Zeichen): {full_text[:300]}")
        
        # Trenne Text in Fragen-Blöcke mit mindestens 10 Unterstrichen
        question_blocks = re.split(r'_{10,}', full_text)
        logger.info(f"Gefunden: {len(question_blocks)} durch Unterstriche getrennte Blöcke")
        
        # Erste Variante: Suche nach "X. Frage:" Format
        # Akzeptiere beide Optionsformate: A) oder A/
        # Erlaube auch eingerückte Fragen
        question_pattern = re.compile(r'^\s*(\d+)\.\s*Frage:?\s*(.*?)(?=(?:\s*[A-E][\)/]|\s*Fach:|\s*Antwort:|\s*Kommentar:|$))', re.DOTALL | re.MULTILINE)
        
        # Gehe durch alle Blöcke
        for block_idx, block in enumerate(question_blocks):
            block = block.strip()
            if not block:
                continue
            
            # Extrahiere die Fragenummer und den Fragetext
            question_match = question_pattern.search(block)
            if not question_match:
                # Alternative Fragemuster (erlaube Einrückung)
                alt_match = re.search(r'^\s*(?:Was|Welche|Wo|Wann|Wie|Warum).*?\?', block, re.DOTALL | re.IGNORECASE | re.MULTILINE)
                if alt_match:
                    question_text = alt_match.group(0).strip()
                    logger.info(f"Alternative Frage gefunden (Block {block_idx+1}): {question_text[:50]}")
                    
                    question_data = {
                        "id": str(uuid.uuid4()),
                        "page": -1,  # Später zuweisen
                        "y": 0,      # Später zuweisen
                        "full_text": block, # Behalte Blocktext
                        "question_number": str(block_idx + 1), # Verwende Blockindex als Nummer
                        "question": question_text,
                        "option_a": "", "option_b": "", "option_c": "", "option_d": "", "option_e": "",
                        "subject": "", "correct_answer": "", "comment": ""
                    }
                    
                    # Extrahiere Optionen A-E (beide Formate: A) oder A/)
                    for letter in "ABCDE":
                        option_match = re.search(rf'{letter}[\)/]\s*(.*?)(?=\s*[A-E][\)/]|\s*Fach:|\s*Antwort:|\s*Kommentar:|$)', block, re.DOTALL | re.IGNORECASE)
                        if option_match:
                            question_data[f"option_{letter.lower()}"] = option_match.group(1).strip()
                    
                    questions.append(question_data)
                continue
            
            # Standardfall: "X. Frage: Text" Format
            question_number = question_match.group(1)
            question_text = question_match.group(2).strip()
            
            logger.info(f"Frage {question_number} gefunden: {question_text[:50]}")
            
            # Vorbereiten der Fragedaten
            question_data = {
                "id": str(uuid.uuid4()),
                "page": -1,  # Später zuweisen
                "y": 0,      # Später zuweisen
                "full_text": block,
                "question_number": question_number,
                "question": question_text,
                "option_a": "", "option_b": "", "option_c": "", "option_d": "", "option_e": "",
                "subject": "", "correct_answer": "", "comment": ""
            }
            
            # Extrahiere Optionen A-E (beide Formate: A) oder A/)
            for letter in "ABCDE":
                option_match = re.search(rf'{letter}[\)/]\s*(.*?)(?=\s*[A-E][\)/]|\s*Fach:|\s*Antwort:|\s*Kommentar:|$)', block, re.DOTALL | re.IGNORECASE)
                if option_match:
                    question_data[f"option_{letter.lower()}"] = option_match.group(1).strip()
            
            # Extrahiere Metadaten
            fach_match = re.search(r'Fach:\s*(.*?)(?=\s*Antwort:|\s*Kommentar:|$)', block, re.DOTALL | re.IGNORECASE)
            if fach_match:
                question_data["subject"] = fach_match.group(1).strip()
            
            antwort_match = re.search(r'Antwort:\s*(.*?)(?=\s*Fach:|\s*Kommentar:|$)', block, re.DOTALL | re.IGNORECASE)
            if antwort_match:
                question_data["correct_answer"] = antwort_match.group(1).strip()
            
            kommentar_match = re.search(r'Kommentar:\s*(.*?)(?=\s*Fach:|\s*Antwort:|$)', block, re.DOTALL | re.IGNORECASE)
            if kommentar_match:
                question_data["comment"] = kommentar_match.group(1).strip()
            
            questions.append(question_data)
        
        # Suche in allen Seiten nach "X. Frage:" für Seitenzuordnung und genaue Y-Position
        # Sortiere Fragen nach Seite und Nummer für die y1-Bestimmung
        questions.sort(key=lambda q: (q.get("page", float('inf')), int(q.get("question_number", 0))))

        for i, q in enumerate(questions):
            # Nur suchen, wenn Seite/Y noch nicht exakt bestimmt wurden
            if q.get("page", -1) == -1 or q.get("y", 0) <= 0:
                # Erstelle verschiedene Suchmuster für den Fall, dass die Frage eingerückt ist
                search_patterns = [
                    f"{q['question_number']}. Frage:",
                    f"\t{q['question_number']}. Frage:",
                    f"  {q['question_number']}. Frage:",
                    f"    {q['question_number']}. Frage:"
                ]
                found = False
                for page_idx in range(len(doc)):
                    page = doc[page_idx]
                    for search_pattern in search_patterns:
                        search_results = page.search_for(search_pattern, quads=True)
                        if search_results:
                            first_quad = search_results[0] # Dies ist ein Quad-Objekt
                            q["page"] = page_idx
                            q["y"] = first_quad.ul.y # Genauere Y-Position (oben)
                            # Speichere auch y1 (unten) des Suchbegriffs
                            q["y1_search_term"] = first_quad.ll.y
                            logger.info(f"Frage {q['question_number']} exakt auf Seite {page_idx+1} bei Y={q['y']:.2f} gefunden.")
                            found = True
                            break
                    if found:
                        break
                if not found:
                    logger.warning(f"Konnte exakte Position für Frage {q['question_number']} nicht finden. Schätzung: Y={q.get('y', 0)}")

        # Schätze y1 (untere Grenze) für jede Frage basierend auf der nächsten Frage
        MIN_TEXT_AREA_HEIGHT = 75  # Min height from bottom of "X. Frage:" text for options/answer
        MARGIN_BELOW_LAST_QUESTION_ON_PAGE = 10
        MARGIN_BEFORE_NEXT_QUESTION_TEXT = 5 # How much space to leave before the text of the next Q starts
        ABSOLUTE_MIN_QUESTION_BLOCK_HEIGHT = 20 # Overall minimum height for a question's y0-y1 block

        page_heights = {p: doc[p].rect.height for p in range(len(doc))} 

        for i, q in enumerate(questions):
            page_idx = q.get("page", -1)
            q_y0 = q.get("y") # Top of "X. Frage:" text block
            q_y1_text_search_marker = q.get("y1_search_term") # Bottom of "X. Frage:" text block

            if page_idx == -1 or q_y0 is None:
                logger.warning(f"Question {q.get('question_number', i)} (ID: {q.get('id')}) on page {page_idx} has no y0 ({q_y0}). Assigning default y1.")
                q["y1"] = (q_y0 or 0) + ABSOLUTE_MIN_QUESTION_BLOCK_HEIGHT
                continue

            base_y_for_min_text_height = q_y1_text_search_marker if q_y1_text_search_marker is not None else q_y0

            next_q_start_y = float('inf')
            for j in range(i + 1, len(questions)):
                next_q = questions[j]
                if next_q.get("page", -1) == page_idx:
                    next_q_start_y = next_q.get("y", float('inf'))
                    break
            
            estimated_y1_based_on_next_q = 0
            if next_q_start_y != float('inf'):
                estimated_y1_based_on_next_q = next_q_start_y - MARGIN_BEFORE_NEXT_QUESTION_TEXT
            else:
                page_height = page_heights.get(page_idx, 842) 
                estimated_y1_based_on_next_q = page_height - MARGIN_BELOW_LAST_QUESTION_ON_PAGE
            
            min_y1_for_text_content = base_y_for_min_text_height + MIN_TEXT_AREA_HEIGHT
            q["y1"] = max(estimated_y1_based_on_next_q, min_y1_for_text_content)
            
            q["y1"] = max(q["y1"], q_y0 + ABSOLUTE_MIN_QUESTION_BLOCK_HEIGHT)

            if next_q_start_y != float('inf') and q["y1"] >= next_q_start_y:
                 q["y1"] = next_q_start_y - MARGIN_BEFORE_NEXT_QUESTION_TEXT 
                 q["y1"] = max(q["y1"], q_y0 + ABSOLUTE_MIN_QUESTION_BLOCK_HEIGHT)

            logger.debug(f"Frage {q.get('question_number')}: Seite {page_idx+1}, Bereich Y0={q_y0:.2f}, Y1={q['y1']:.2f}")

        # Zweite Variante: Wenn keine oder nur wenige Fragen gefunden wurden, suche nach Fragezeichen-Sätzen
        if len(questions) < 5:
            logger.warning(f"Nur {len(questions)} Fragen gefunden. Versuche alternativen Ansatz (Fragezeichen)...")
            
            # Verwende den gesamten Text, um Sätze zu finden
            try:
                full_doc_text = doc.get_text()
                # Erlaube eingerückte Fragesätze
                question_sentences = re.findall(r'^\s*(?:[^.!?]*?(?:Was|Welche|Wo|Wann|Wie|Warum)[^.!?]*?\?)', full_doc_text, re.IGNORECASE | re.MULTILINE)
                valid_sentences = [s.strip() for s in question_sentences if len(s.strip()) > 20]

                # Füge diese als Fragen hinzu, aber ohne genaue Position
                for idx, sentence in enumerate(valid_sentences, start=len(questions)+1):
                    questions.append({
                        "id": str(uuid.uuid4()),
                        "page": -1, # Position unbekannt
                        "y": 0,     # Position unbekannt
                        "full_text": sentence, # Ganzer Satz als Text
                        "question_number": str(idx),
                        "question": sentence,
                        "option_a": "", "option_b": "", "option_c": "", "option_d": "", "option_e": "",
                        "subject": "", "correct_answer": "", "comment": ""
                        # Optionen etc. können hier nicht zuverlässig extrahiert werden
                    })
                logger.info(f"{len(valid_sentences)} Fragen über Fragezeichen-Muster hinzugefügt.")
            except Exception as text_error:
                logger.error(f"Fehler beim Extrahieren des Volltextes für Fragezeichen-Suche: {text_error}")

        logger.info(f"Insgesamt {len(questions)} Fragen extrahiert")
        return questions

    finally:
        # Schließe das Dokument nur, wenn es hier geöffnet wurde
        if close_doc_at_end and doc:
            doc.close()

def parse_question_details(question):
    """
    Parst zusätzliche Details, falls diese in der Extraktion noch nicht erfasst wurden
    """
    try:
        full_text = question.get("full_text", "")
        logger.info(f"Parse Details für Frage {question.get('question_number', '?')}, full_text Länge: {len(full_text)}")
        
        # Debug: Zeige erste 500 Zeichen des full_text
        logger.debug(f"Full text preview für Frage {question.get('question_number', '?')}: {full_text[:500]}")
        
        # Extrahiere Optionen, falls noch nicht geschehen
        options = {}
        
        # Verbesserte Option-Extraktion: Suche zuerst alle Option-Marker
        option_markers = list(re.finditer(r'^\s*([A-E])[\)/]', full_text, re.MULTILINE))
        logger.info(f"Gefundene Option-Marker für Frage {question.get('question_number', '?')}: {[m.group(0).strip() for m in option_markers]}")
        
        # Methode 1: Versuche zeilenbasierte Extraktion
        lines = full_text.split('\n')
        logger.info(f"Analysiere {len(lines)} Zeilen für Optionen in Frage {question.get('question_number', '?')}")
        
        for i, line in enumerate(lines):
            # Behalte führende/nachfolgende Leerzeichen für besseres Debugging
            original_line = line
            line = line.strip()
            
            # Debug erste paar Zeilen
            if i < 10:
                logger.debug(f"Zeile {i}: '{line}'")
            
            # Prüfe ob die Zeile mit einem Options-Marker beginnt (erlaube Einrückung)
            option_match = re.match(r'^\s*([A-E])[\)/]\s*(.*)', line)
            if option_match:
                letter = option_match.group(1)
                content = option_match.group(2).strip()
                
                # Sammle Text von nachfolgenden Zeilen, bis die nächste Option oder Metadaten gefunden werden
                j = i + 1
                content_lines = [content] if content else []
                
                while j < len(lines):
                    next_line = lines[j] # Behalte Einrückung für die Logik
                    # Prüfe ob die nächste Zeile eine neue Option oder Metadaten ist
                    if re.match(r'^\s*[A-E][\)/]', next_line) or re.match(r'^\s*(Fach|Antwort|Kommentar):', next_line, re.IGNORECASE):
                        break
                    
                    if next_line.strip():
                        content_lines.append(next_line.strip())
                    j += 1
                
                # Setze i auf die letzte verarbeitete Zeile, um Doppelverarbeitung zu vermeiden
                i = j - 1
                
                options[letter] = " ".join(content_lines).strip()
                logger.debug(f"Option {letter}: '{options[letter][:50]}...'")
        
        # Methode 2: Falls keine Optionen gefunden, verwende die ursprüngliche Regex
        if not options:
            logger.info("Zeilenbasierte Extraktion fand keine Optionen, verwende Regex-Methode")
            option_matches = re.finditer(r'([A-E])[\)/]\s*(.*?)(?=\s*[A-E][\)/]|\s*Fach:|\s*Antwort:|\s*Kommentar:|$)', full_text, re.DOTALL)
            for match in option_matches:
                options[match.group(1)] = match.group(2).strip()
        
        # Debug-Ausgabe für Optionen
        logger.info(f"Extrahierte {len(options)} Optionen für Frage {question.get('question_number', '?')}: {options}")
        
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

def extract_images_with_coords(doc: fitz.Document): # Akzeptiert doc statt pdf_path
    """
    Optimierte Bildextraktionsfunktion mit Performance-Verbesserungen
    """
    # Entferne: doc = fitz.open(pdf_path)
    images = []
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
                    base_image = doc.extract_image(xref) # Verwende doc
                    
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
                            # Extrahiere xref (funktioniert für viele PDF-Versionen)
                            xref = block.get("xref", 0)
                            if xref == 0 and 'image' in block and isinstance(block['image'], bytes):
                                 # Manchmal ist das Bild direkt im Block (selten)
                                 # Diese Logik ist komplexer, da 'items' analysiert werden müssten
                                 logger.warning(f"Bild in Block {block_idx+1} ohne xref gefunden, überspringe vorerst.")
                                 continue

                            # Überspringe bereits extrahierte Bilder
                            if xref in extracted_xrefs:
                                 logger.info(f"Bild mit xref {xref} bereits extrahiert (Methode 2), überspringe...")
                                 continue

                            if xref:
                                 extracted_xrefs.add(xref)
                                 base_image = doc.extract_image(xref) # Verwende doc

                                 if not base_image: continue

                                 image_bytes = base_image["image"]
                                 image_ext = base_image["ext"]

                                 if len(image_bytes) < 100:
                                     logger.warning(f"Bild auf Seite {page_number+1} zu klein ({len(image_bytes)} Bytes), überspringe...")
                                     continue

                                 bbox = [0, 0, 0, 0]
                                 if "bbox" in block and isinstance(block["bbox"], (list, tuple)) and len(block["bbox"]) >= 4:
                                     bbox = list(block["bbox"])
                                 else:
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
    
    # Prüfe Bildqualität und Größe
    if images:
        logger.info(f"Beispiel-Bildgröße: {len(images[0]['image_bytes'])} Bytes, Format: {images[0]['image_ext']}")
    
    return images

def map_images_to_questions(questions: List[Dict], images: List[Dict], doc: fitz.Document) -> List[Dict]:
    """
    Assigns images to the closest question on the same page based on vertical proximity.
    An image is primarily assigned to a question if its vertical midpoint falls within the question's y0-y1 range.
    If multiple such questions exist, the one with the smallest vertical span is chosen.
    If no question 'contains' the image, it's assigned to the question with the minimum 
    absolute vertical distance between their respective bounding box edges.
    The 'doc' parameter is currently unused by this mapping logic but kept for signature consistency.
    """
    logger.info(f"Starting advanced image-to-question mapping for {len(images)} images and {len(questions)} questions.")
    questions_by_page: Dict[int, List[Dict]] = {}
    for q_idx, q in enumerate(questions):
        page = q.get("page", -1)
        y0 = q.get("y") 
        y1 = q.get("y1") # y1 is the bottom of the question block

        if page < 0 or y0 is None or y1 is None or y1 < y0:
            logger.debug(f"Question {q.get('question_number', q_idx)} (ID: {q.get('id')}) skipped due to invalid page/y-coords (Page: {page}, Y0: {y0}, Y1: {y1}).")
            continue
        questions_by_page.setdefault(page, []).append(q)

    assigned_image_count = 0
    for img_idx, img in enumerate(images):
        img_page = img.get("page", -1)
        img_bbox = img.get("bbox")

        if img_page not in questions_by_page or not isinstance(img_bbox, (list, tuple)) or len(img_bbox) < 4:
            logger.debug(f"Image {img_idx} skipped due to invalid page/bbox (Page: {img_page}, Bbox: {img_bbox}).")
            continue

        img_y0 = img_bbox[1]
        img_y1 = img_bbox[3]
        img_mid_y = (img_y0 + img_y1) / 2

        candidate_questions_for_img = questions_by_page[img_page]
        
        # 1. Check for questions that "contain" the image's midpoint
        containing_questions = []
        for q in candidate_questions_for_img:
            q_y0 = q["y"]
            q_y1 = q["y1"]
            if q_y0 <= img_mid_y <= q_y1:
                containing_questions.append(q)
        
        best_q = None
        if containing_questions:
            # If contained by multiple, pick the one with the smallest vertical span (tightest fit)
            best_q = min(containing_questions, key=lambda q: q["y1"] - q["y"])
            logger.debug(f"Image {img_idx} (Page {img_page}, MidY {img_mid_y:.2f}) is contained by question {best_q.get('question_number', best_q.get('id'))} (Y0:{best_q['y']:.2f}-Y1:{best_q['y1']:.2f}).")

        # 2. If not contained, find the question with the minimum absolute vertical distance
        if not best_q:
            min_dist = float('inf')
            for q in candidate_questions_for_img:
                q_y0 = q["y"]
                q_y1 = q["y1"]
                
                # Calculate distance:
                # Distance is 0 if image and question overlap vertically.
                # Otherwise, it's the gap between the closest edges.
                dist = 0
                if img_y1 < q_y0: # Image is entirely above question
                    dist = q_y0 - img_y1
                elif img_y0 > q_y1: # Image is entirely below question
                    dist = img_y0 - q_y1
                # else: they overlap, distance is 0 or could be negative if fully contained.
                # For non-containing, we are interested in positive separation.
                
                if dist < min_dist:
                    min_dist = dist
                    best_q = q
            if best_q:
                 logger.debug(f"Image {img_idx} (Page {img_page}, Y0:{img_y0:.2f}-Y1:{img_y1:.2f}) assigned to closest non-containing question {best_q.get('question_number', best_q.get('id'))} (Y0:{best_q['y']:.2f}-Y1:{best_q['y1']:.2f}) with distance {min_dist:.2f}.")


        if best_q:
            question_id = best_q["id"]
            img["question_id"] = question_id
            
            # QUICK FIX: Assign image to the question one number lower than determined
            # This compensates for the systematic offset where images are assigned one question too high
            original_question_number = best_q.get('question_number', '0')
            try:
                target_question_number = str(int(original_question_number) - 1)
                # Find the question with the target number on the same page
                target_question = None
                for q in candidate_questions_for_img:
                    if q.get('question_number') == target_question_number:
                        target_question = q
                        break
                
                if target_question:
                    # Reassign to the target question (one number lower)
                    question_id = target_question["id"]
                    img["question_id"] = question_id
                    best_q = target_question  # Update best_q for logging and key generation
                    logger.info(f"QUICK FIX: Reassigned image {img_idx} from question {original_question_number} to question {target_question_number}")
                else:
                    logger.warning(f"QUICK FIX: Could not find target question {target_question_number} on page {img_page}, keeping original assignment to question {original_question_number}")
            except (ValueError, TypeError):
                logger.warning(f"QUICK FIX: Could not parse question number '{original_question_number}' as integer, keeping original assignment")
            
            # Key construction uses image's midpoint Y, as before
            img_ext = img.get("image_ext", "jpg")
            key_y_component = int(img_mid_y) 
            image_key = f"{question_id}_{img_page}_{key_y_component}.{img_ext}"
            
            # Set image_key on the question. If a question gets multiple images,
            # the last one processed for that question will set its image_key.
            # This is consistent with previous logic.
            best_q["image_key"] = image_key 
            assigned_image_count +=1
            logger.info(f"Image {img_idx} (Page {img_page}, MidY {img_mid_y:.2f}) successfully mapped to question {best_q.get('question_number', best_q.get('id'))} (ID: {question_id}). Image Key: {image_key}")
        else:
            logger.warning(f"Image {img_idx} on page {img_page} could not be mapped to any question.")

    logger.info(f"Advanced image-to-question mapping complete: {assigned_image_count} of {len(images)} images assigned.")
    return images

def insert_questions_into_db(questions, exam_name, exam_year, exam_semester, user_id, visibility, university_id, config):
    """
    Fügt Fragen in Supabase im Bulk-Modus ein für bessere Performance
    Handles university assignment based on visibility and university_id
    """
    global current_pdf_filename
    supabase = config.supabase
    successful = 0
    failed = 0
    
    logger.info(f"Füge {len(questions)} Fragen in Supabase ein, Datei: {current_pdf_filename}")
    
    # Log university assignment details
    if visibility == "university" and university_id:
        logger.info(f"University assignment: {len(questions)} questions will be assigned to university {university_id}")
    elif visibility == "university" and not university_id:
        logger.warning(f"University visibility requested but no university_id provided. Questions will be saved as private.")
        visibility = "private"  # Fallback to private
    elif visibility == "private":
        logger.info(f"Private assignment: {len(questions)} questions will be saved as private (no university assignment)")
    
    # Verwende den globalen Dateinamen oder einen Fallback
    pdf_filename = current_pdf_filename
    if not pdf_filename:
        # Fallback-Dateiname generieren (sollte selten vorkommen)
        pdf_filename = f"{exam_name}_{exam_year}_{exam_semester}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        logger.warning(f"Kein Dateiname vorhanden, verwende generierten Namen: {pdf_filename}")
    
    # Bereite Daten für Bulk-Upload vor
    bulk_data = []
    skipped_in_db_prep = 0
    
    for q in questions:
        # Die Struktur kommt jetzt von formatted_questions, parse_question_details ist hier nicht mehr nötig
        # (wurde bereits in process_pdf angewendet)
            
        # Sicherstellen, dass alle Werte Strings sind
        options_dict = q.get("options", {}) # Hole das verschachtelte Optionen-Dict
        
        # Finale Validierung vor DB-Insert - sollte eigentlich nicht nötig sein, aber zur Sicherheit
        question_text = str(q.get("question", "")).strip()
        if len(question_text) < 5:
            logger.warning(f"DB-Prep: Überspringe Frage mit zu kurzem Text beim DB-Insert: '{question_text}'")
            skipped_in_db_prep += 1
            continue
            
        # Prüfe Optionen nochmals
        options_list = [
            str(options_dict.get("A", "")).strip(),
            str(options_dict.get("B", "")).strip(),
            str(options_dict.get("C", "")).strip(),
            str(options_dict.get("D", "")).strip(),
            str(options_dict.get("E", "")).strip()
        ]
        non_empty_options = [opt for opt in options_list if opt]
        
        if len(non_empty_options) == 0:
            logger.warning(f"DB-Prep: Überspringe Frage ohne Optionen beim DB-Insert: '{question_text[:50]}...'")
            skipped_in_db_prep += 1
            continue

        # Determine final university_id based on visibility and provided university_id
        final_university_id = None
        if visibility == "university" and university_id:
            final_university_id = str(university_id)

        data = {
            "id": str(q.get("id", uuid.uuid4())),
            "exam_name": str(exam_name or ""),
            "exam_year": str(exam_year or ""),
            "exam_semester": str(exam_semester or ""),
            "question": question_text,  # Verwende validierte Version
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
            "visibility": str(visibility), # Add visibility
            "university_id": final_university_id # Add university_id (None if not university visibility)
        }
        bulk_data.append(data)
    
    if skipped_in_db_prep > 0:
        logger.warning(f"DB-Insert-Vorbereitung: {skipped_in_db_prep} weitere Fragen wurden beim finalen Check übersprungen.")
    
    # Log final assignment summary
    university_assigned_count = sum(1 for item in bulk_data if item["university_id"] is not None)
    private_count = len(bulk_data) - university_assigned_count
    logger.info(f"Database assignment summary: {university_assigned_count} questions assigned to university, {private_count} questions as private")
    
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
        # Öffne Doc hier für main, wenn extract* es braucht
        doc = fitz.open(pdf_path)
        questions = extract_questions_with_coords(doc) # Pass doc
        for q in questions:
            parse_question_details(q)
        print(f"{len(questions)} Fragen extrahiert.")

        # Bilder verarbeiten (MinIO beibehalten)
        images = extract_images_with_coords(doc) # Pass doc
        images = map_images_to_questions(questions, images, doc) # Pass doc
        doc.close() # Schließe doc hier in main
        print(f"{len(images)} Bilder extrahiert.")

        bucket_name = "exam-images"
        # Bild-Upload Logik basierend auf image_key in questions
        processed_keys = set()
        for q in questions:
             if q.get("image_key") and q["image_key"] not in processed_keys:
                 image_key = q["image_key"]
                 # Finde das Bild, das zu diesem Key gehört
                 found_img_data = None
                 for img in images:
                      # Finde Key basierend auf Bild-Metadaten
                      img_y_for_key = 0
                      bbox = img.get("bbox", [0, 0])[1]
                      try: img_y_for_key = int(float(bbox))
                      except: pass
                      expected_key = f"{q.get('id')}_{img.get('page', 0)}_{img_y_for_key}.{img.get('image_ext', 'jpg')}"
                      if expected_key == image_key and img.get("image_bytes"):
                           found_img_data = img["image_bytes"]
                           break
                 if found_img_data:
                      upload_successful = upload_image_to_supabase(
                          found_img_data,
                          image_key,
                          bucket_name,
                          config
                      )
                      if upload_successful:
                           processed_keys.add(image_key)
                           print(f"Bild {image_key} hochgeladen.")
                      else:
                           print(f"Fehler beim Upload von {image_key}.")
                 else:
                      print(f"Keine Bilddaten für Key {image_key} gefunden.")


        # Fragen in Supabase speichern
        insert_questions_into_db(questions, exam_name, exam_year, exam_semester, None, "private", None, config)
        
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
                "Frage-Muster 1": r"^\s*\d+\.\s*Frage:",
                "Frage-Muster 2": r"^\s*Frage\s*\d+[:\.]",
                "Optionen": r"^\s*[A-E]\)",
                "Unterstrichtrennungen": r"_{5,}",
            }
            
            for name, pattern in patterns.items():
                matches = re.findall(pattern, text)
                logger.info(f"{name}: {len(matches)} Treffer - Beispiele: {matches[:3]}")
    
    return {"pages": len(doc), "analyzed_samples": sample_pages}

# --- Neue Hilfsfunktion ---
def find_separator_lines(doc: fitz.Document) -> Dict[int, List[float]]:
    """
    Findet horizontale Linien (wahrscheinliche Trenner) in einem PDF-Dokument.

    Args:
        doc: Das fitz.Document Objekt.

    Returns:
        Ein Dictionary, bei dem die Schlüssel die Seitenzahlen (0-basiert) sind
        und die Werte Listen von Y-Koordinaten der gefundenen Trennlinien auf dieser Seite.
    """
    separators_by_page = {}
    min_line_width_ratio = 0.7  # Mindestbreite der Linie im Verhältnis zur Seitenbreite
    max_line_height = 5         # Maximale Höhe der Linie

    for page_idx, page in enumerate(doc):
        page_lines = []
        page_width = page.rect.width
        drawings = page.get_drawings()

        for path in drawings:
            # Prüfe, ob es sich um ein gefülltes Rechteck handelt (oft für Linien verwendet)
            if path["type"] == "f" and path["rect"]:
                rect = path["rect"]
                # Prüfe, ob es eine lange, dünne horizontale Linie ist
                if (rect.width / page_width >= min_line_width_ratio and
                        rect.height <= max_line_height):
                    # Verwende die obere Y-Koordinate der Linie
                    page_lines.append(rect.y0)
            # Optional: Prüfe auch Linienpfade ('s'), falls Trenner so gezeichnet werden
            elif path["type"] == "s":
                 # Diese Logik ist komplexer, da 'items' analysiert werden müssten
                 # Vorerst konzentrieren wir uns auf Rechtecke ('f')
                 pass

        if page_lines:
            # Sortiere Linien nach Y-Position und entferne Duplikate (nahe Linien)
            page_lines.sort()
            unique_lines = []
            if page_lines:
                last_y = -1
                for y in page_lines:
                    if y - last_y > max_line_height: # Nur hinzufügen, wenn weit genug entfernt
                       unique_lines.append(y)
                       last_y = y
            separators_by_page[page_idx] = unique_lines
            logger.info(f"Seite {page_idx+1}: {len(unique_lines)} Trennlinien gefunden.")

    return separators_by_page

def should_ignore_question(question_data: Dict) -> bool:
    """
    Prüft, ob eine Frage ignoriert werden soll basierend auf bestimmten Kriterien.
    
    Ignoriert Fragen die:
    - Keinen oder nur sehr kurzen Fragetext haben (< 5 Zeichen)
    - Keine Antwortoptionen haben (mindestens eine Option muss vorhanden sein)
    - Nur "Gesucht: richtig/falsch?" oder ähnliche Platzhalter enthalten
    
    Args:
        question_data: Dictionary mit Fragedaten
        
    Returns:
        True wenn die Frage ignoriert werden soll, False sonst
    """
    question_text = question_data.get("question", "").strip()
    
    # 1. Prüfe auf leeren oder zu kurzen Fragetext
    if len(question_text) < 5:
        logger.info(f"Ignoriere Frage mit zu kurzem Text (< 5 Zeichen): '{question_text}'")
        return True
    
    # 2. Prüfe auf "Gesucht: richtig/falsch" Muster (mit oder ohne Fragezeichen)
    gesucht_patterns = [
        "gesucht: richtig/falsch?",
        "gesucht: richtig/falsch",
        "gesucht:richtig/falsch?", 
        "gesucht:richtig/falsch"
    ]
    
    # Normalisiere den Text für Vergleich (lowercase, entferne extra Leerzeichen)
    normalized_question = re.sub(r'\s+', ' ', question_text.lower().strip())
    
    # 3. Prüfe, ob die Frage mindestens eine Antwortoption hat
    options = [
        question_data.get("option_a", "").strip(),
        question_data.get("option_b", "").strip(), 
        question_data.get("option_c", "").strip(),
        question_data.get("option_d", "").strip(),
        question_data.get("option_e", "").strip()
    ]
    
    # Debug: Zeige extrahierte Optionen
    logger.info(f"Prüfe Optionen für Frage '{question_text[:30]}...': A='{options[0][:20] if options[0] else '(leer)'}', B='{options[1][:20] if options[1] else '(leer)'}', C='{options[2][:20] if options[2] else '(leer)'}', D='{options[3][:20] if options[3] else '(leer)'}', E='{options[4][:20] if options[4] else '(leer)'}'")
    
    # Zähle nicht-leere Optionen
    non_empty_options = [opt for opt in options if opt]
    
    # Keine Optionen = ignorieren
    if len(non_empty_options) == 0:
        logger.info(f"Ignoriere Frage ohne Antwortoptionen: '{question_text[:50]}...'")
        logger.debug(f"Full question data: {question_data}")
        return True
    
    # Prüfe, ob die Frage nur aus einem der "Gesucht" Muster besteht
    for pattern in gesucht_patterns:
        if normalized_question == pattern:
            logger.info(f"Ignoriere Frage mit nur 'Gesucht: richtig/falsch' Text: '{question_text}'")
            return True
    
    # 4. Zusätzliche Prüfung: Frage besteht nur aus Zahlen oder Sonderzeichen
    if re.match(r'^[\d\s\.\-\_]+$', question_text):
        logger.info(f"Ignoriere Frage die nur aus Zahlen/Sonderzeichen besteht: '{question_text}'")
        return True
    
    return False

def extract_content_from_docx(doc: docx.Document) -> Tuple[List[Dict], List[Dict]]:
    """Extrahiert Fragen und Bilder aus einem DOCX-Dokument mit korrekter Positionsverfolgung."""
    
    def get_paragraph_list_info(paragraph):
        """Extrahiert List-Informationen aus einem Word-Absatz."""
        try:
            # Methode 1: Prüfe numPr (numbering properties)
            if hasattr(paragraph, '_element') and hasattr(paragraph._element, 'pPr'):
                pPr = paragraph._element.pPr
                if pPr is not None:
                    # Suche nach numPr Element
                    numPr = pPr.find('.//w:numPr', namespaces={'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'})
                    if numPr is not None:
                        # Dies ist ein nummerierter/Aufzählungs-Absatz
                        ilvl = numPr.find('.//w:ilvl', namespaces={'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'})
                        level = int(ilvl.get('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}val', '0')) if ilvl is not None else 0
                        logger.debug(f"NumPr gefunden: Level {level}")
                        return True, level
            
            # Methode 2: Prüfe den Paragraph Style
            if hasattr(paragraph, 'style') and paragraph.style:
                style_name = paragraph.style.name.lower() if paragraph.style.name else ""
                # Typische List-Style-Namen
                if any(keyword in style_name for keyword in ['list', 'bullet', 'number', 'enumerat']):
                    logger.debug(f"List-Style erkannt: {paragraph.style.name}")
                    return True, 0
                    
        except Exception as e:
            logger.debug(f"Fehler beim Extrahieren der List-Info: {e}")
        return False, 0
    
    questions = []
    images = []
    current_question = None
    current_block_text = ""
    document_position = 0
    list_item_counter = {}  # Zähler für List-Items pro Level
    
    # Erstelle eine Map von Bild-IDs zu Bilddaten
    image_map = {}
    for rel_id, rel in doc.part.rels.items():
        if "image" in rel.target_ref:
            image_bytes = rel.target_part.blob
            ext = rel.target_part.content_type.split('/')[-1]
            image_map[rel_id] = {
                "image_bytes": image_bytes,
                "image_ext": ext if ext else 'png',
                "rel_id": rel_id
            }
    
    logger.info(f"DOCX enthält {len(image_map)} Bilder")
    
    # Sequenziell durch das Dokument gehen
    for para_idx, paragraph in enumerate(doc.paragraphs):
        text = paragraph.text.strip()
        
        # Prüfe ob dieser Absatz Teil einer nummerierten Liste ist
        is_list_item, list_level = get_paragraph_list_info(paragraph)
        list_label = ""
        
        if is_list_item and current_question:
            # Erhöhe den Zähler für dieses Level
            if list_level not in list_item_counter:
                list_item_counter[list_level] = 0
            
            # Wenn wir zu einem niedrigeren Level zurückkehren, setze höhere Level zurück
            for level in list(list_item_counter.keys()):
                if level > list_level:
                    del list_item_counter[level]
            
            list_item_counter[list_level] += 1
            counter = list_item_counter[list_level]
            
            # Erstelle das Label basierend auf dem Zähler (A), B), etc.)
            if counter <= 5:  # Nur A-E
                list_label = f"{chr(64 + counter)})"  # A), B), etc.
                logger.debug(f"Liste erkannt in Absatz {para_idx}: Level {list_level}, Label: {list_label}")
        
        # Wenn eine neue Frage beginnt, setze die List-Zähler zurück
        if re.match(r'^\s*(\d+)\.\s*Frage:?\s*(.*)', text, re.IGNORECASE):
            list_item_counter.clear()
        
        # Debug: Zeige die ersten paar Absätze
        if para_idx < 20 or (current_question and current_question.get("question_number") == "52"):
            logger.debug(f"Absatz {para_idx}: '{text[:100]}...' (Länge: {len(text)}, aktuelle Frage: {current_question['question_number'] if current_question else 'None'}, List: {list_label})")
        
        # Prüfe auf Unterstrich-Trennlinie (Fragenende)
        if re.match(r'_{10,}$', text):
            logger.info(f"Trennlinie gefunden bei Absatz {para_idx}")
            # Verarbeite die aktuelle Frage, falls vorhanden
            if current_question:
                # Entferne die Trennlinie selbst vom Text
                if current_block_text.endswith(text + "\n"):
                    current_block_text = current_block_text[:-len(text)-1]
                
                current_question["full_text"] = current_block_text.strip()
                logger.info(f"Trennlinie gefunden - speichere Frage {current_question['question_number']} mit {len(current_block_text)} Zeichen")
                logger.debug(f"Full text für Frage {current_question['question_number']}: {current_block_text[:200]}...")
                parse_question_details(current_question)
                questions.append(current_question)
                logger.info(f"Frage {current_question['question_number']} abgeschlossen bei Position {document_position}")
            
            current_question = None
            current_block_text = ""
            document_position += 1
            list_item_counter.clear()  # Reset list counters
            continue
        
        # Füge Text zum aktuellen Block hinzu (auch leere Zeilen behalten)
        # Wenn es eine nummerierte Liste ist, füge das Label voran
        if list_label and current_question:
            current_block_text += f"{list_label} {text}\n"
            logger.info(f"Füge List-Item zu Frage {current_question['question_number']} hinzu: {list_label} {text[:50]}...")
        else:
            current_block_text += text + "\n"
        
        # Prüfe auf Fragebeginn (erlaube Einrückung)
        question_match = re.match(r'^\s*(\d+)\.\s*Frage:?\s*(.*)', text, re.IGNORECASE)
        if question_match:
            # Wenn wir bereits eine Frage verarbeiten, speichere sie erst
            if current_question:
                # Entferne die neue Fragezeile vom current_block_text bevor wir die alte Frage speichern
                temp_block_text = current_block_text
                if temp_block_text.endswith(text + "\n"):
                    temp_block_text = temp_block_text[:-len(text)-1]
                
                temp_block_text = temp_block_text.strip()
                if temp_block_text:
                    current_question["full_text"] = temp_block_text
                    logger.info(f"Verarbeite vorherige Frage {current_question['question_number']}, Text-Länge: {len(temp_block_text)}")
                    parse_question_details(current_question)
                    questions.append(current_question)
                    logger.info(f"Frage {current_question['question_number']} abgeschlossen (neue Frage gefunden)")
            
            # Starte eine neue Frage
            question_number = question_match.group(1)
            question_text = question_match.group(2).strip()
            
            current_question = {
                "id": str(uuid.uuid4()),
                "question_number": question_number,
                "question": question_text if question_text else "",  # Fragetext könnte in nächster Zeile sein
                "document_position": document_position,
                "full_text": "",
                "option_a": "", "option_b": "", "option_c": "", "option_d": "", "option_e": "",
                "subject": "", "correct_answer": "", "comment": ""
            }
            current_block_text = text + "\n"  # Reset block text mit aktuellem Text
            document_position += 1
            list_item_counter.clear()  # Reset list counters for new question
            logger.info(f"Neue Frage {question_number} bei Position {document_position} gefunden")
        
        # Prüfe auf Bilder in diesem Absatz
        for run in paragraph.runs:
            # Überprüfe inline shapes (Bilder)
            if hasattr(run._element, 'drawing_lst'):
                for drawing in run._element.drawing_lst:
                    # Extrahiere die Bild-Referenz
                    for blip in drawing.iter('{http://schemas.openxmlformats.org/drawingml/2006/main}blip'):
                        embed_id = blip.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed')
                        if embed_id and embed_id in image_map:
                            image_data = image_map[embed_id].copy()
                            image_data["document_position"] = document_position
                            image_data["associated_question_number"] = current_question["question_number"] if current_question else None
                            images.append(image_data)
                            logger.info(f"Bild bei Position {document_position} gefunden, zugeordnet zu Frage {image_data['associated_question_number']}")
                            document_position += 1
        
        # Alternative Methode für Bilder (für andere DOCX-Strukturen)
        if hasattr(paragraph._element, 'r_lst'):
            for r in paragraph._element.r_lst:
                for drawing in r.findall('.//w:drawing', namespaces={'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}):
                    for blip in drawing.findall('.//a:blip', namespaces={'a': 'http://schemas.openxmlformats.org/drawingml/2006/main'}):
                        embed_id = blip.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed')
                        if embed_id and embed_id in image_map:
                            # Prüfe ob dieses Bild schon hinzugefügt wurde
                            already_added = any(img.get('rel_id') == embed_id and img.get('document_position') == document_position for img in images)
                            if not already_added:
                                image_data = image_map[embed_id].copy()
                                image_data["document_position"] = document_position
                                image_data["associated_question_number"] = current_question["question_number"] if current_question else None
                                images.append(image_data)
                                logger.info(f"Bild (alternative Methode) bei Position {document_position} gefunden, zugeordnet zu Frage {image_data['associated_question_number']}")
                                document_position += 1
    
    # Verarbeite die letzte Frage, falls noch nicht gespeichert
    if current_question and current_block_text.strip():
        current_question["full_text"] = current_block_text.strip()
        logger.info(f"Speichere letzte Frage {current_question['question_number']} mit {len(current_block_text)} Zeichen")
        parse_question_details(current_question)
        questions.append(current_question)
        logger.info(f"Letzte Frage {current_question['question_number']} abgeschlossen")
    
    logger.info(f"Extraktion abgeschlossen: {len(questions)} Fragen und {len(images)} Bilder mit Positionsinformationen")
    
    # Debug: Zeige Zusammenfassung der extrahierten Fragen
    for q in questions:
        has_options = any(q.get(f"option_{letter}", "") for letter in "abcde")
        # Prüfe ob Optionen mit List-Labels extrahiert wurden
        has_list_options = any(
            re.match(r'^[A-E]\)', q.get(f"option_{letter}", "").strip()[:2]) 
            for letter in "abcde" 
            if q.get(f"option_{letter}", "")
        )
        logger.info(f"Frage {q['question_number']}: Hat Optionen={has_options}, Hat List-Format={has_list_options}, Text-Länge={len(q.get('full_text', ''))}, Fragetext='{q.get('question', '')[:50]}...'")
        
        # Debug: Zeige die ersten paar Zeichen jeder Option
        if has_options:
            for letter in "abcde":
                opt = q.get(f"option_{letter}", "")
                if opt:
                    logger.debug(f"  Option {letter.upper()}: '{opt[:30]}...'")
    
    return questions, images

def map_images_to_questions_docx(questions: List[Dict], images: List[Dict]) -> List[Dict]:
    """Ordnet Bilder Fragen in einem DOCX-Dokument zu, basierend auf ihrer Position im Dokument."""
    if not images or not questions:
        return images

    # Erstelle eine Map von Fragenummern zu Fragen-IDs
    question_map = {q["question_number"]: q for q in questions}
    
    logger.info(f"Starte DOCX Bild-zu-Frage-Zuordnung für {len(images)} Bilder")
    
    assigned_count = 0
    for img_idx, img in enumerate(images):
        # Die Zuordnung wurde bereits während der Extraktion gemacht
        associated_question_number = img.get("associated_question_number")
        
        if associated_question_number and associated_question_number in question_map:
            question = question_map[associated_question_number]
            question_id = question["id"]
            img["question_id"] = question_id
            
            # Erstelle einen eindeutigen image_key
            image_key = f"{question_id}_docx_{img_idx}.{img.get('image_ext', 'png')}"
            
            # Wenn die Frage bereits einen image_key hat (mehrere Bilder), füge eine Nummer hinzu
            if question.get("image_key"):
                # Zähle wie viele Bilder bereits dieser Frage zugeordnet wurden
                existing_images = sum(1 for i in images[:img_idx] if i.get("question_id") == question_id)
                image_key = f"{question_id}_docx_{existing_images + 1}.{img.get('image_ext', 'png')}"
            
            question["image_key"] = image_key
            img["image_key"] = image_key
            assigned_count += 1
            
            logger.info(f"Bild {img_idx} erfolgreich zu Frage {associated_question_number} (ID: {question_id}) zugeordnet. Key: {image_key}")
        else:
            logger.warning(f"Bild {img_idx} konnte keiner Frage zugeordnet werden (associated_question_number: {associated_question_number})")
    
    logger.info(f"DOCX Bild-zu-Frage-Zuordnung abgeschlossen: {assigned_count} von {len(images)} Bildern zugeordnet")
    
    return images

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        workers=1,
        log_level="info"
    )
