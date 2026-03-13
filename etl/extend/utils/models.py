from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class Product(BaseModel):
    name: str
    unlimitedStock: Optional[bool] = None
    stockLevel: int
    skuCode: Optional[str] = None
    eanCode: Optional[str] = None
    articleCode: Optional[str] = None
    price: Optional[float] = None
    remoteId: Optional[str] = None
    remoteDataSyncedToDate: Optional[datetime] = None
    status: Optional[str] = None
    assembled: Optional[bool] = None
    createdAtRemote: Optional[datetime] = None
    notBeingBought: Optional[bool] = None
    minimumStock: Optional[int] = None


class ProductCompositions(BaseModel):
    composedProductId: str
    partProductId: str
    partQuantity: int
    remoteId: Optional[str] = None
    remoteDataSyncedToDate: datetime


class Supplier(BaseModel):
    name: str
    remoteId: str
    emails: Optional[List[str]] = None
    ignored: Optional[bool] = None
    deliveryTime: Optional[int] = None
    fixedCosts: Optional[float] = None
    remoteDataSyncedToDate: datetime
    userReplenishmentPeriod: Optional[int] = None


class SupplierProduct(BaseModel):
    name: str
    remoteId: str
    price: Optional[float] = None
    skuCode: Optional[str] = None
    articleCode: Optional[str] = None
    status: Optional[str] = None
    eanCode: Optional[str] = None
    lotSize: Optional[int] = None
    minimumPurchaseQuantity: Optional[int] = None
    deliveryTime: Optional[int] = None
    productId: str
    supplierId: str
    remoteDataSyncedToDate: Optional[datetime] = None
    preferred: Optional[bool] = None
    weight: Optional[float] = None
    volume: Optional[float] = None
    freeStock: Optional[int] = None


class SellOrderWithLines(BaseModel):
    totalValue: float
    remoteId: Optional[str] = None
    placed: datetime
    orderLines: list
    completed: Optional[datetime] = None
    remoteDataSyncedToDate: Optional[datetime] = None


class SellOrder(BaseModel):
    totalValue: float
    remoteId: Optional[str] = None
    placed: datetime
    completed: Optional[datetime] = None
    remoteDataSyncedToDate: Optional[datetime] = None


class SellOrderLine(BaseModel):
    quantity: int
    subtotalValue: float
    productId: str
    remoteId: Optional[str] = None


class BuyOrder(BaseModel):
    remoteId: str
    supplierId: str
    placed: datetime
    totalValue: float
    remoteDataSyncedToDate: Optional[datetime] = None
    completed: Optional[datetime] = None
    expectedDeliveryDate: Optional[datetime] = None


class BuyOrderLine(BaseModel):
    quantity: int
    subtotalValue: float
    buyOrderId: int
    productId: int
    remoteId: Optional[str] = None
    remoteDataSyncedToDate: Optional[datetime] = None
    expectedDeliveryDate: Optional[datetime] = None


class ReceiptLine(BaseModel):
    occurred: datetime
    quantity: int
    buyOrderLineId: str
    remoteId: Optional[str] = None
    remoteDataSyncedToDate: Optional[datetime] = None
