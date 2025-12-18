from typing import Sequence
from sqlalchemy.orm import Session
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from datetime import date
from ..models.persona import Persona
from ..views.persona import PersonaCreate, PersonaUpdate
from .errors import PersonaNotFoundError, EmailAlreadyExistsError

from faker import Faker 
import random 
fake = Faker('es_ES')


def create_persona(db: Session, payload: PersonaCreate) -> Persona:
    """Create a Persona ensuring unique email."""
    # Optimistic check; DB unique constraint is the final guard
    if db.query(Persona).filter(Persona.email == payload.email).first():
        raise EmailAlreadyExistsError()
    obj = Persona(
        first_name=payload.first_name,
        last_name=payload.last_name,
        email=payload.email,
        phone=payload.phone,
        birth_date=payload.birth_date,
        is_active=payload.is_active,
        notes=payload.notes,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        # Catch race conditions on unique email
        raise EmailAlreadyExistsError() from e
    db.refresh(obj)
    return obj


def poblar_personas(db: Session, cantidad: int) -> int:
    """ Usando Faker, poblamos la base para una cantidad dada."""
    
    dominios_validos = ["gmail.com", "outlook.com", "yahoo.com", "hotmail.com"]
    registros = []
    for _ in range(cantidad):
        first_name = fake.first_name()
        last_name = fake.last_name()
        dominio = random.choice(dominios_validos)
        email = f"{first_name.lower()}.{last_name.lower()}@{dominio}"

        persona = Persona(
            first_name=first_name,
            last_name=last_name,
            email=email,
            phone=fake.phone_number(),
            birth_date=fake.date_of_birth(minimum_age=18, maximum_age=90),
            is_active=random.choice([True, False]),
            notes=random.choice([fake.sentence(nb_words=6), None])
        )
        registros.append(persona)
        
    db.add_all(registros)
    db.commit()
    return len(registros)

def get_stats_dominios(db: Session) -> dict:
    """Punto C: Retorna conteo de usuarios por dominio de correo[cite: 53]."""
    personas = db.query(Persona).all()
    conteo = {}
    for p in personas:
        try:
        
            dominio = p.email.split('@')[1]
            conteo[dominio] = conteo.get(dominio, 0) + 1
        except (IndexError, AttributeError):
            continue
    return conteo

def get_stats_edad(db: Session) -> dict:
    """Punto D: Calcula edad promedio, mínima y máxima basada en birth_date."""
    personas = db.query(Persona).all()
    if not personas:
        return {"edad_promedio": 0, "edad_minima": 0, "edad_maxima": 0}
    
    hoy = date.today()
    edades = []
    for p in personas:
        if p.birth_date:
            # Cálculo de edad: año actual menos año de nacimiento 
            edad = hoy.year - p.birth_date.year - ((hoy.month, hoy.day) < (p.birth_date.month, p.birth_date.day))
            edades.append(edad)
    
    if not edades:
        return {"edad_promedio": 0, "edad_minima": 0, "edad_maxima": 0}

    return {
        "edad_promedio": sum(edades) // len(edades),
        "edad_minima": min(edades),
        "edad_maxima": max(edades)
    }

def search_personas(db: Session, termino: str) -> Sequence[Persona]:
    """Punto E: Busca el término en first_name, last_name O email."""
    return db.query(Persona).filter(
        or_( # Aplicación del operador OR solicitado 
            Persona.first_name.contains(termino),
            Persona.last_name.contains(termino),
            Persona.email.contains(termino)
        )
    ).all()
def list_personas(db: Session, skip: int = 0, limit: int = 100) -> Sequence[Persona]:
    """Return paginated list of Personas."""
    return db.query(Persona).offset(skip).limit(limit).all()


def get_persona(db: Session, persona_id: int) -> Persona:
    """Return Persona by ID or raise if not found."""
    obj = db.query(Persona).filter(Persona.id == persona_id).first()
    if not obj:
        raise PersonaNotFoundError()
    return obj


def update_persona(db: Session, persona_id: int, payload: PersonaUpdate) -> Persona:
    """Update Persona partially, enforcing unique email."""
    obj = db.query(Persona).filter(Persona.id == persona_id).first()
    if not obj:
        raise PersonaNotFoundError()

    data = payload.model_dump(exclude_unset=True)
    if "email" in data and data["email"] != obj.email:
        if db.query(Persona).filter(Persona.email == data["email"], Persona.id != persona_id).first():
            raise EmailAlreadyExistsError()

    for field, value in data.items():
        setattr(obj, field, value)

    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise EmailAlreadyExistsError() from e
    db.refresh(obj)
    return obj


def delete_persona(db: Session, persona_id: int) -> None:
    """Delete Persona by ID or raise if not found."""
    obj = db.query(Persona).filter(Persona.id == persona_id).first()
    if not obj:
        raise PersonaNotFoundError()
    db.delete(obj)
    db.commit()


def reset_personas(db: Session) -> int:
    """Delete all Personas and return count."""
    deleted_count = db.query(Persona).delete()
    db.commit()
    return deleted_count
