# --------------------------------------------------------------
# input topic, LLM processor, output processor
# --------------------------------------------------------------

# This has a system dependency on poppler
# brew install poppler

from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import argparse

from models import DocumentWrapper
from models import PatientObject
import json
import os
import logging
from llama_stack_client import LlamaStackClient
from pdf2image import convert_from_path
from pdf2image.exceptions import PDFInfoNotInstalledError, PDFPageCountError, PDFSyntaxError
from pathlib import Path

import base64

# Load env vars
load_dotenv()
KAFKA_INPUT_TOPIC=os.getenv("KAFKA_PATIENT_INPUT_TOPIC") 
KAFKA_BROKER=os.getenv("KAFKA_BROKER")
KAFKA_OUTPUT_TOPIC=os.getenv("KAFKA_PATIENT_OUTPUT_TOPIC")
KAFKA_REVIEW_TOPIC=os.getenv("KAFKA_REVIEW_TOPIC")

LLAMA_STACK_SERVER=os.getenv("LLAMA_STACK_SERVER")
LLAMA_STACK_VISION_MODEL=os.getenv("LLAMA_STACK_VISION_MODEL")

client = LlamaStackClient(base_url=os.getenv("LLAMA_STACK_SERVER"))


# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)

logger.info(f"Kafka bootstrap servers: {KAFKA_BROKER}")
logger.info(f"Kafka input topic: {KAFKA_INPUT_TOPIC}")
logger.info(f"Kafka output topic: {KAFKA_OUTPUT_TOPIC}")
logger.info(f"LlamaStack server: {LLAMA_STACK_SERVER}")
logger.info(f"LlamaStack vision model: {LLAMA_STACK_VISION_MODEL}")


class MessageProcessor():
    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_INPUT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            # group_id='patient_structure',
            # enable_auto_commit=True
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )


    def make_done_path(self,pdf_path: str) -> str:
        """
        Given a path like:
        /…/data/intake/patient-intake-form-2.pdf
        returns:
        /…/data/intake/done/patient-intake-form-2.pdf.done
        """
        p = Path(pdf_path)
        # target directory: same as parent, but with “done” subfolder
        done_dir = p.parent / "done"
        # ensure the string ends with .done
        done_name = p.name + ".done"
        return str(done_dir / done_name)

    def convert_pdf_to_png(self, pdf_path, output_dir="./data/pngs"):
        """
        Converts each page of a PDF file to a PNG image.

        Args:
            pdf_path (str): The path to the input PDF file.
            output_dir (str, optional): The directory to save the PNG files.
                                        Defaults to the directory of the PDF file.
        """
        if not os.path.exists(pdf_path):
            print(f"Error: PDF file not found at {pdf_path}")
            return

        if not output_dir:
            output_dir = os.path.dirname(pdf_path)
            if not output_dir: # Handle case where only filename is given
                output_dir = '.'

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        try:
            print(f"Converting {pdf_path} to PNG images...")
            # Convert PDF to a list of PIL images
            images = convert_from_path(pdf_path)

            # Get the base name of the PDF file without extension
            base_filename = os.path.splitext(os.path.basename(pdf_path))[0]

            # Save each image as a PNG file
            for i, image in enumerate(images):
                output_filename = os.path.join(output_dir, f"{base_filename}_page_{i + 1}.png")
                image.save(output_filename, 'PNG')
                print(f"Saved page {i + 1} to {output_filename}")

            print("Conversion complete.")

            return output_filename

        except PDFInfoNotInstalledError:
            print("Error: pdf2image requires poppler to be installed and in PATH.")
            print("Please install poppler:")
            print("  macOS (brew): brew install poppler")
            print("  Debian/Ubuntu: sudo apt-get install poppler-utils")
            print("  Windows: Download from https://github.com/oschwartz10612/poppler-windows/releases/")
        except PDFPageCountError:
            print(f"Error: Could not get page count for {pdf_path}. Is it a valid PDF?")
        except PDFSyntaxError:
            print(f"Error: PDF file {pdf_path} seems to be corrupted or invalid.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    
    def encode_image(self, image_path):
        with open(image_path, "rb") as image_file:
            base64_string = base64.b64encode(image_file.read()).decode("utf-8")        
            return base64_string
    
    def strip_double_asterisks(self, s: str) -> str:
        """
        If s starts and ends with '**', remove those markers.
        Otherwise, return s unchanged.
        """
        if s.startswith("**") and s.endswith("**"):
            return s[2:-2]
        return s    

    def strip_trailing_dot(self, s: str) -> str:
        """
        Remove one '.' at the end of the string if present.
        """
        if s.endswith('.'):
            return s[:-1]
        return s    
    
    # Takes the input, modifies, returns it back    
    def process(self, message:DocumentWrapper) -> DocumentWrapper:
        try:
            logger.info("LLM File Name: " + message.filename)
            logger.info("LLM File Path: " + message.metadata["original_path"])

            pdf_path = message.metadata["original_path"]
            logger.info(f"Converting PDF to PNG images: type {type(pdf_path)}")

            # The original pdf was moved and marked as .done so we need to pick up its new name/location
            done_path = self.make_done_path(pdf_path)
            logger.info(f"Done path: {done_path}")
            
            png_path = self.convert_pdf_to_png(done_path)
            logger.info(f"PNG path: {png_path}")

            encoded_image = self.encode_image(png_path)

            # -------------------------------------------------------
            # LLM Magic Happens
            # -------------------------------------------------------
        
            client = LlamaStackClient(base_url=os.getenv("LLAMA_STACK_SERVER"))

            response = client.inference.chat_completion(
                model_id=LLAMA_STACK_VISION_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "image": {
                                    "data": encoded_image
                                }
                            },
                            {
                                "type": "text",
                                "text": "what is patients's last name, only the last name",
                            }
                        ]
                    }
                ],                    
            )
            last_name = self.strip_double_asterisks(response.completion_message.content)
            last_name = self.strip_trailing_dot(last_name)
            logger.info(f"last_name: {last_name}")

            response = client.inference.chat_completion(
                model_id=LLAMA_STACK_VISION_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "image": {
                                    "data": encoded_image
                                }
                            },
                            {
                                "type": "text",
                                "text": "what is patients's first name? Return only the [first_name]",
                            }
                        ]
                    }
                ],                    
            )
            first_name = self.strip_double_asterisks(response.completion_message.content)
            first_name = self.strip_trailing_dot(first_name)
            logger.info(f"first_name: {first_name}")

            response = client.inference.chat_completion(
                model_id=LLAMA_STACK_VISION_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "image": {
                                    "data": encoded_image
                                }
                            },
                            {
                                "type": "text",
                                "text": "what is patients's date of birth, return only the date of birth",
                            }
                        ]
                    }
                ],                    
            )
            date_of_birth = self.strip_double_asterisks(response.completion_message.content)
            date_of_birth = self.strip_trailing_dot(date_of_birth)
            logger.info(f"date_of_birth: {date_of_birth}")

            response = client.inference.chat_completion(
                model_id=LLAMA_STACK_VISION_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "image": {
                                    "data": encoded_image
                                }
                            },
                            {
                                "type": "text",
                                "text": "what is patients's phone number, return only the phone number",
                            }
                        ]
                    }
                ],                    
            )
            phone = self.strip_double_asterisks(response.completion_message.content)
            phone = self.strip_trailing_dot(phone)
            logger.info(f"phone: {phone}")

            response = client.inference.chat_completion(
                model_id=LLAMA_STACK_VISION_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "image": {
                                    "data": encoded_image
                                }
                            },
                            {
                                "type": "text",
                                "text": "what is patients's insurance company? If you can not find it, return [unknown]",
                            }
                        ]
                    }
                ],                    
            )
            insurance_provider = self.strip_double_asterisks(response.completion_message.content)
            logger.info(f"insurance_provider: {insurance_provider}")


            patient = PatientObject(
                last_name=last_name,
                first_name=first_name,
                date_of_birth=date_of_birth,
                phone=phone,
                insurance_provider=insurance_provider
            )
            logger.info(f"patient: {patient}")

            message.structured=patient
            # -------------------------------------------------------
            # LLM Magic Happens
            # -------------------------------------------------------

            return message
        except Exception as e:
            # Need to say something about what when wrong
            logger.error(f"BAD Thing: {e}")
            return message
    
    def to_review(self, message: DocumentWrapper):
        try:
            self.producer.send(KAFKA_REVIEW_TOPIC, message.model_dump())
            self.producer.flush()
            logger.info(f"Message sent to topic: {KAFKA_REVIEW_TOPIC}")
        except Exception as e:
            logger.error(f"Error sending message to topic {KAFKA_REVIEW_TOPIC}: {str(e)}")
        
    # -------------------------------------------------------
    # Action Happens
    # -------------------------------------------------------
    def run(self):       
        try:
            logger.info("Starting message processor...")
            for kafka_message in self.consumer:
                logger.info(f"Before Processing message: {type(kafka_message)}")                
                # Extract the JSON payload from the Kafka message
                message_data = kafka_message.value  # `value` contains the deserialized JSON payload
                # logger.info(f"Message data type: {type(message_data)}")
                # logger.info(f"Message data: {message_data}")

                # Convert JSON data into a Pydantic Message object
                try:
                    message = DocumentWrapper(**message_data)
                except Exception as e:
                    logger.error(f"Failed to create Message object: {str(e)}")
                    logger.error(f"Message data that caused error: {message_data}")
                    raise

                # Process the message
                processed_message = self.process(message)
                # logger.info(f"After Processing message: {processed_message}")
                # logger.info(f"After JSON: {processed_message.model_dump_json()}")
                # # if we fail to extract structure send to review topic
                # Send the message to output
                self.producer.send(KAFKA_OUTPUT_TOPIC, processed_message.model_dump())

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            error_message = DocumentWrapper(
                id="error",
                filename="error.txt",
                content=str(e),
                error=[str(e)]
            )
            self.to_review(error_message)
        finally:
          self.consumer.close()
          self.producer.close()
          logger.info("Closed Kafka connections")

if __name__ == "__main__":
    processor = MessageProcessor()
    processor.run()
