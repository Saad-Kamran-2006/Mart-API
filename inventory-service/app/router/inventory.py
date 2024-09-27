from fastapi import APIRouter, Depends, Form, HTTPException
from sqlmodel import Session, select
from app.config.db import get_session
from app.models.inventory_model import Get_Inventory, Inventory, Edit_Inventory
from typing import Annotated

inventory_router = APIRouter(
    prefix="/inventory", tags=["inventory"], responses={404: {"description": "Not Found"}}
)

@inventory_router.get("/inventories", response_model=list[Get_Inventory])
async def get_all_inventory(session: Annotated[Session, Depends(get_session)]):
    inventories: Get_Inventory = session.exec(select(Inventory)).all()
    if inventories:
        return inventories
    else:
        raise HTTPException(status_code=404, detail="No inventory found")


@inventory_router.get("/{product_id}", response_model=Get_Inventory)
async def get_single_product(product_id: str, session: Annotated[Session, Depends(get_session)]):
    inventory: Get_Inventory = session.exec(select(Inventory).where(Inventory.product_id == product_id)).first()
    if inventory:
        return inventory
    else:
        raise HTTPException(status_code=404, detail="No inventory found")


@inventory_router.put("/{product_id}", response_model=Get_Inventory)
async def edit_product(
    product_id: str,
    inventory_data: Annotated[Edit_Inventory, Depends()],
    session: Annotated[Session, Depends(get_session)],
):
    existing_inventory = session.exec(select(Inventory).where(Inventory.product_id == product_id)).first()

    if existing_inventory:
        existing_inventory.quantity = inventory_data.quantity
        
        session.add(existing_inventory)
        session.commit()
        session.refresh(existing_inventory)
        return existing_inventory
    else:
        raise HTTPException(status_code=404, detail="No inventory found")