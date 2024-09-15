from pydantic import BaseModel

class OrderData(BaseModel):
    export_code: str
    store_transfer_code: str
    item_code: str
    item_name: str
    quantity: int
    create_date_d: str
    create_by: str
