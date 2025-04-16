# --------------------------------------------------------------
# input topic, LLM processor, output processor
# --------------------------------------------------------------

from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from openai import OpenAI
from models import DocumentWrapper
from models import SelectedRoute
from models import Route
import json
import os
import logging
from openai import OpenAI
import struct # Import struct, although not directly used, good practice if related errors occur


# Load env vars
load_dotenv()
KAFKA_BROKER=os.getenv("KAFKA_BROKER")
KAFKA_INPUT_TOPIC=os.getenv("KAFKA_ROUTER_INPUT_TOPIC") 
KAFKA_ROUTER_OUTPUT_PATIENT_TOPIC=os.getenv("KAFKA_ROUTER_OUTPUT_PATIENT_TOPIC")
KAFKA_ROUTER_OUTPUT_CONTRACT_TOPIC=os.getenv("KAFKA_ROUTER_OUTPUT_CONTRACT_TOPIC")
KAFKA_ROUTER_OUTPUT_REPORT_TOPIC=os.getenv("KAFKA_ROUTER_OUTPUT_REPORT_TOPIC")
KAFKA_ROUTER_OUTPUT_INVOICE_TOPIC=os.getenv("KAFKA_ROUTER_OUTPUT_INVOICE_TOPIC")
KAFKA_REVIEW_TOPIC=os.getenv("KAFKA_REVIEW_TOPIC")
MODEL_NAME=os.getenv("ROUTER_MODEL_NAME")
API_KEY=os.getenv("ROUTER_API_KEY")
INFERENCE_SERVER_URL=os.getenv("ROUTER_SERVER_URL")

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


# Safer deserializer function
def safe_json_deserializer(x):
    if not x:  # Check if the byte string is empty
        logger.warning("Received empty message value from Kafka, skipping.")
        return None # Indicate empty/invalid message
    try:
        return json.loads(x.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}. Message bytes: {x!r}")
        return None # Indicate decode error
    except UnicodeDecodeError as e:
        logger.error(f"Failed to decode UTF-8: {e}. Message bytes: {x!r}")
        return None # Indicate decode error
    except Exception as e:
        logger.error(f"Unexpected error in deserializer: {e}. Message bytes: {x!r}")
        return None # Indicate other errors

logger.info(f"INFERENCE_SERVER_URL: {INFERENCE_SERVER_URL}")
logger.info(f"MODEL_NAME: {MODEL_NAME}")
logger.info(f"Kafka bootstrap servers: {KAFKA_BROKER}")
logger.info(f"Kafka input topic: {KAFKA_INPUT_TOPIC}")
logger.info(f"Kafka output Patient topic: {KAFKA_ROUTER_OUTPUT_PATIENT_TOPIC}")
logger.info(f"Kafka output Contract topic: {KAFKA_ROUTER_OUTPUT_CONTRACT_TOPIC}")
logger.info(f"Kafka output Report topic: {KAFKA_ROUTER_OUTPUT_REPORT_TOPIC}")
logger.info(f"Kafka output Invoice topic: {KAFKA_ROUTER_OUTPUT_INVOICE_TOPIC}")
logger.info(f"Kafka review topic: {KAFKA_REVIEW_TOPIC}")


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
            group_id='document_router',
            enable_auto_commit=True
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )

    # Takes the input, modifies, returns it back    
    def process(self, message:DocumentWrapper) -> SelectedRoute:
        try:
            logger.info("LLM Processing: " + message.content)

            # -------------------------------------------------------
            # LLM Magic Happens
            # -------------------------------------------------------
            content_lower = message.content.lower()

            completion = client.beta.chat.completions.parse(
                model=MODEL_NAME,
                messages=[
                    {
                    "role": "system", "content": 
                    """ You are an AI-powered message classifier for an enterprise support system. 
                    Your task is to analyze documented and determine the most appropriate team for handling them:
                    - **Patient**: Patient intake documents and related forms.
                    - **Contract**: Contracts.
                    - **Report**: Sales reports.
                    - **Invoice**: Invoices.
                    - **Unknown**: If the message does not fit into any of the above categories or lacks sufficient context to classify accurately.
                    """
                    },
                    {
                    "role": "user",
                    "content": "Classify the following message and determine the appropriate routing category (Patient, Contract, Report, Invoice,or Unknown).\n" 
                            + content_lower,
                    },                
                ],
                temperature=0.0, 
                response_format=SelectedRoute,
            )
            response= completion.choices[0].message.parsed            
            logger.info(f"LLM response type: {type(response)}")
            logger.info(f"Route selected: {response.route}")
            # -------------------------------------------------------
            # LLM Magic Happens
            # -------------------------------------------------------

            return response
        except Exception as e:
            # Need to say something about what when wrong
            logger.error(f"BAD Thing: {e}")
            return None
        
    
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
                logger.info(f"Received raw Kafka message: {type(kafka_message)}")
                
                message_data = kafka_message.value
                
                if message_data is None:
                    logger.warning("Skipping message due to deserialization error or empty content.")
                    continue # Skip to the next message

                logger.info(f"Message data type after deserialization: {type(message_data)}")
                logger.info(f"Message data: {message_data}")

                # Convert JSON data into a Pydantic Message object
                try:
                    message = DocumentWrapper(**message_data)
                    # Process the message
                    selected_route = self.process(message)
                    logger.info(f"selected_route: {selected_route.route}")
                    logger.info(f"selected_route type: {type(selected_route.route)}")
                except Exception as e:
                    logger.error(f"Failed to create Message object: {str(e)}")
                    logger.error(f"Message data that caused error: {message_data}")
                    raise

                if selected_route.route == Route.patient:
                    logger.info("Routing message to Patient Team.")
                    message.route = Route.patient
                    self.producer.send(KAFKA_ROUTER_OUTPUT_PATIENT_TOPIC, message.model_dump())
                elif selected_route.route == Route.contract:
                    logger.info("Routing message to Contract Team.")
                    message.route = Route.contract
                    self.producer.send(KAFKA_ROUTER_OUTPUT_CONTRACT_TOPIC, message.model_dump())
                elif selected_route.route == Route.report:
                    logger.info("Routing message to Report Team.")
                    message.route = Route.report
                    self.producer.send(KAFKA_ROUTER_OUTPUT_REPORT_TOPIC, message.model_dump())
                elif selected_route.route == Route.invoice:
                    logger.info("Routing message to Invoice Team.")
                    message.route = Route.invoice
                    self.producer.send(KAFKA_ROUTER_OUTPUT_INVOICE_TOPIC, message.model_dump())
                else:
                    logger.info("Message classification is Unknown. Sending for review.")
                    message.route = Route.unknown
                    self.to_review(message)

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            error_message = DocumentWrapper(
                id="error",
                filename="error.txt",
                content=str(e),
                error=[str(e)]
            )
            self.to_review(error_message)
        # finally:
        #   self.consumer.close()
        #   self.producer.close()
        #   logger.info("Closed Kafka connections")

if __name__ == "__main__":
    processor = MessageProcessor()
    processor.run()
