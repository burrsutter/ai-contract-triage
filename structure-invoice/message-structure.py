# --------------------------------------------------------------
# input topic, LLM processor, output processor
# --------------------------------------------------------------

from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from openai import OpenAI
from models import DocumentWrapper
from models import InvoiceObject
import json
import os
import logging
from openai import OpenAI


# Load env vars
load_dotenv()
KAFKA_INPUT_TOPIC=os.getenv("KAFKA_INVOICE_INPUT_TOPIC") 
KAFKA_BROKER=os.getenv("KAFKA_BROKER")
KAFKA_OUTPUT_TOPIC=os.getenv("KAFKA_INVOICE_OUTPUT_TOPIC")
KAFKA_REVIEW_TOPIC=os.getenv("KAFKA_REVIEW_TOPIC")
MODEL_NAME=os.getenv("STRUCTURE_MODEL_NAME")
API_KEY=os.getenv("STRUCTURE_API_KEY")
INFERENCE_SERVER_URL=os.getenv("STRUCTURE_SERVER_URL")

client = OpenAI(
    api_key=API_KEY,
    base_url=INFERENCE_SERVER_URL
    )


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


llmclient = OpenAI(
    api_key=API_KEY,
    base_url=INFERENCE_SERVER_URL
    )


class MessageProcessor():
    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_INPUT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='invoice_structure',
            enable_auto_commit=True
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )

    # Takes the input, modifies, returns it back    
    def process(self, message:DocumentWrapper) -> DocumentWrapper:
        try:
            logger.info("LLM Structure Processing: " + message.content)

            # -------------------------------------------------------
            # LLM Magic Happens
            # -------------------------------------------------------

            completion = client.beta.chat.completions.parse(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": "Extract the invoice information."},
                    {"role": "user", "content": message.content},
                ],
                response_format=InvoiceObject,
            )
            
            invoice = completion.choices[0].message.parsed
            if invoice: 
                logger.info(f"analysis type: {type(invoice)}")
                logger.info("------------------------")
                # logger.info(emailanalysis)
                logger.info(f"Invoice Number: {invoice.invoice_number}")
                logger.info(f"Invoice Date:   {invoice.invoice_date}")
                logger.info(f"Invoice Amount: {invoice.invoice_amount}")                
                logger.info(f"Bill To:       {invoice.bill_to}")
                logger.info(f"Ship To:       {invoice.ship_to}")
                logger.info(f"Payment Terms: {invoice.payment_terms}")                
                logger.info("------------------------")

                message.structured=invoice
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
                logger.info(f"Message data type: {type(message_data)}")
                logger.info(f"Message data: {message_data}")

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
