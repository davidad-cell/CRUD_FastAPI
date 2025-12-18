from typing import List
from fastapi import APIRouter, Depends, Query, status,  HTTPException
from sqlalchemy.orm import Session

from ..database import get_db
from ..views.persona import PersonaCreate, PersonaUpdate, PersonaRead, PoblarRequest, PoblarResponse
from ..services import persona_service

router = APIRouter(prefix="/personas", tags=["personas"])


@router.post("", response_model=PersonaRead, status_code=status.HTTP_201_CREATED)
def create_persona(persona_in: PersonaCreate, db: Session = Depends(get_db)):
    """Create a new Persona delegating to service layer."""
    # Let domain errors bubble up to global handlers
    return persona_service.create_persona(db, persona_in)

@router.post("/poblar", response_model=PoblarResponse, status_code=status.HTTP_201_CREATED)
def poblar_personas(payload: PoblarRequest, db: Session = Depends(get_db)):
    cantidad = payload.cantidad
    if cantidad <= 0 or cantidad > 1000:
        raise HTTPException(status_code=400, detail="La cantidad debe ser mayor a 0 y menor o igual a 1000.")
    count = persona_service.poblar_personas(db, cantidad)
    return PoblarResponse(
        message=f"Se insertaron {count} registros ficticios en la base de datos.",
        inserted_count=count
    )

@router.get("/estadisticas/dominios", status_code=status.HTTP_200_OK)
def get_stats_dominios(db: Session = Depends(get_db)):
    """Punto C: Retorna conteo de usuarios por dominio de correo[cite: 52]."""
    return persona_service.get_stats_dominios(db)

@router.get("/estadisticas/edad", status_code=status.HTTP_200_OK)
def get_stats_edad(db: Session = Depends(get_db)):
    """Punto D: Calcula estadísticas de edad (promedio, min, max)[cite: 60]."""
    return persona_service.get_stats_edad(db)

@router.get("/buscar/{termino}", status_code=status.HTTP_200_OK)
def search_personas(termino: str, db: Session = Depends(get_db)):
    """Punto E: Busca término en nombre, apellido o email[cite: 70]."""
    return persona_service.search_personas(db, termino)

@router.get("", response_model=List[PersonaRead])
def list_personas(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """List Personas with pagination via service layer."""
    return persona_service.list_personas(db, skip=skip, limit=limit)


@router.get("/{persona_id}", response_model=PersonaRead)
def get_persona(persona_id: int, db: Session = Depends(get_db)):
    """Retrieve a Persona by ID via service layer."""
    return persona_service.get_persona(db, persona_id)


@router.put("/{persona_id}", response_model=PersonaRead)
def update_persona(persona_id: int, persona_in: PersonaUpdate, db: Session = Depends(get_db)):
    """Update an existing Persona (partial) via service layer."""
    return persona_service.update_persona(db, persona_id, persona_in)


@router.delete("/reset", status_code=status.HTTP_200_OK)
def reset_personas(db: Session = Depends(get_db)):
    """Elimina todos los registros de la tabla personas."""
    count = persona_service.reset_personas(db)
    return {
        "message": "Base de datos limpiada. Se eliminaron todos los registros.",
        "deleted_count": count
    }


@router.delete("/{persona_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_persona(persona_id: int, db: Session = Depends(get_db)):
    """Delete a Persona by ID via service layer."""
    persona_service.delete_persona(db, persona_id)
    return None
