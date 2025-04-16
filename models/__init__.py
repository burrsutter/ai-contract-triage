from pydantic import BaseModel, Field


from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum


class Route(str, Enum):
    contract = "contract"
    patient = "patient"
    report = "report"
    invoice = "invoice"
    unknown = "unknown"

class SelectedRoute(BaseModel):
    route: Route
    reason: str
    escalation_required: bool

class DocumentWrapper(BaseModel):
    id: str
    filename: str
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)    
    comment: Optional[str] = None
    route: Optional[Route] = None
    structured: Optional[Any] = None
    error: Optional[list] = Field(default_factory=list)

class InvoiceObject(BaseModel):
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_amount: Optional[float] = None
    bill_to: Optional[str] = None
    ship_to: Optional[str] = None     
    payment_terms: Optional[str] = None
    

class ContractType(str, Enum):
    amzn = "aws"
    crm = "salesforce"
    msft = "azure"
    now = "servicenow"
    fieldsales = "fieldsales"

class ContractObject(BaseModel):
    contract_type: Optional[ContractType] = None
    contract_number: Optional[str] = None
    contract_start_date: Optional[datetime] = None
    contract_end_date: Optional[datetime] = None
    contract_status: Optional[str] = None
    contract_value: Optional[float] = None
