from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn
import os
import shutil
from datetime import datetime
from typing import List, Optional
import uuid
import json
from pydantic import BaseModel
from enum import Enum
from database import db_manager

app = FastAPI(title="MileTracker API", version="1.0.0")

# Enable CORS for Flutter web
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your Flutter app's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create directories for file storage
os.makedirs("uploads/receipts", exist_ok=True)

# Mount static files
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

# Data models
class TripType(str, Enum):
    personal = "personal"
    business = "business"

class Trip(BaseModel):
    id: int
    date: str
    startTime: str
    endTime: str
    startLocation: str
    endLocation: str
    distance: float
    potential: float
    type: TripType
    notes: str = ""

class TripCreate(BaseModel):
    date: str
    startTime: str
    endTime: str
    startLocation: str
    endLocation: str
    distance: float
    potential: float
    type: TripType
    notes: str = ""

class Receipt(BaseModel):
    id: str
    url: str
    name: str
    date: datetime
    tripId: Optional[int] = None

class Location(BaseModel):
    latitude: float
    longitude: float
    timestamp: int

class TripUpdate(BaseModel):
    type: Optional[TripType] = None
    notes: Optional[str] = None

class ReceiptTag(BaseModel):
    tripId: Optional[int] = None

# Startup and shutdown events
@app.on_event("startup")
async def startup():
    await db_manager.connect()
    await db_manager.create_tables()

@app.on_event("shutdown")
async def shutdown():
    await db_manager.disconnect()

@app.get("/")
def read_root():
    return {"message": "MileTracker API is up and running!"}

# Trip endpoints
@app.get("/api/trips", response_model=List[Trip])
async def get_trips():
    trips_data = await db_manager.get_all_trips()
    # Convert database field names to match the model
    trips = []
    for trip in trips_data:
        trips.append({
            "id": trip["id"],
            "date": trip["date"],
            "startTime": trip["start_time"],
            "endTime": trip["end_time"],
            "startLocation": trip["start_location"],
            "endLocation": trip["end_location"],
            "distance": trip["distance"],
            "potential": trip["potential"],
            "type": trip["type"],
            "notes": trip["notes"]
        })
    return trips

@app.get("/api/trips/{trip_id}", response_model=Trip)
async def get_trip(trip_id: int):
    trip = await db_manager.get_trip_by_id(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    
    return {
        "id": trip["id"],
        "date": trip["date"],
        "startTime": trip["start_time"],
        "endTime": trip["end_time"],
        "startLocation": trip["start_location"],
        "endLocation": trip["end_location"],
        "distance": trip["distance"],
        "potential": trip["potential"],
        "type": trip["type"],
        "notes": trip["notes"]
    }

@app.post("/api/trips", response_model=Trip)
async def create_trip(trip_data: TripCreate):
    trip_dict = {
        "date": trip_data.date,
        "start_time": trip_data.startTime,
        "end_time": trip_data.endTime,
        "start_location": trip_data.startLocation,
        "end_location": trip_data.endLocation,
        "distance": trip_data.distance,
        "potential": trip_data.potential,
        "type": trip_data.type.value,
        "notes": trip_data.notes
    }
    
    created_trip = await db_manager.create_trip(trip_dict)
    # Update stats after creating a trip
    await db_manager.update_trip_stats()
    
    return {
        "id": created_trip["id"],
        "date": created_trip["date"],
        "startTime": created_trip["start_time"],
        "endTime": created_trip["end_time"],
        "startLocation": created_trip["start_location"],
        "endLocation": created_trip["end_location"],
        "distance": created_trip["distance"],
        "potential": created_trip["potential"],
        "type": created_trip["type"],
        "notes": created_trip["notes"]
    }

@app.put("/api/trips/{trip_id}", response_model=Trip)
async def update_trip(trip_id: int, trip_update: TripUpdate):
    update_data = {}
    if trip_update.type is not None:
        update_data["type"] = trip_update.type.value
    if trip_update.notes is not None:
        update_data["notes"] = trip_update.notes
    
    updated_trip = await db_manager.update_trip(trip_id, **update_data)
    if not updated_trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    
    # Update stats after modifying a trip
    await db_manager.update_trip_stats()
    
    return {
        "id": updated_trip["id"],
        "date": updated_trip["date"],
        "startTime": updated_trip["start_time"],
        "endTime": updated_trip["end_time"],
        "startLocation": updated_trip["start_location"],
        "endLocation": updated_trip["end_location"],
        "distance": updated_trip["distance"],
        "potential": updated_trip["potential"],
        "type": updated_trip["type"],
        "notes": updated_trip["notes"]
    }

@app.delete("/api/trips/{trip_id}")
async def delete_trip(trip_id: int):
    success = await db_manager.delete_trip(trip_id)
    if not success:
        raise HTTPException(status_code=404, detail="Trip not found")
    
    # Update stats after deleting a trip
    await db_manager.update_trip_stats()
    return {"message": "Trip deleted successfully"}

@app.get("/api/trips/stats")
async def get_trip_stats():
    stats = await db_manager.get_trip_stats()
    return {
        "totalDrives": int(stats.get("total_drives", 0)),
        "totalMiles": int(stats.get("total_miles", 0)),
        "totalLogged": int(stats.get("total_logged", 0))
    }

# Receipt endpoints
@app.get("/api/receipts", response_model=List[Receipt])
async def get_receipts():
    receipts_data = await db_manager.get_all_receipts()
    receipts = []
    for receipt in receipts_data:
        receipts.append({
            "id": receipt["id"],
            "url": receipt["url"],
            "name": receipt["name"],
            "date": receipt["date"],
            "tripId": receipt["trip_id"]
        })
    return receipts

@app.post("/api/receipts/upload")
async def upload_receipt(file: UploadFile = File(...)):
    if not file.content_type or not file.content_type.startswith('image/'):
        raise HTTPException(status_code=400, detail="File must be an image")
    
    # Generate unique filename
    file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'jpg'
    unique_filename = f"{uuid.uuid4()}.{file_extension}"
    file_path = f"uploads/receipts/{unique_filename}"
    
    # Save file
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Get file size
    file_size = os.path.getsize(file_path)
    
    # Create receipt record in database
    receipt_data = {
        "id": str(uuid.uuid4()),
        "url": f"/uploads/receipts/{unique_filename}",
        "name": file.filename,
        "date": datetime.now(),
        "trip_id": None,
        "file_size": file_size,
        "mime_type": file.content_type
    }
    
    created_receipt = await db_manager.create_receipt(receipt_data)
    
    return {
        "id": created_receipt["id"],
        "url": created_receipt["url"],
        "name": created_receipt["name"],
        "date": created_receipt["date"],
        "tripId": created_receipt["trip_id"]
    }

@app.put("/api/receipts/{receipt_id}/tag")
async def tag_receipt(receipt_id: str, tag_data: ReceiptTag):
    updated_receipt = await db_manager.update_receipt_trip(receipt_id, tag_data.tripId)
    if not updated_receipt:
        raise HTTPException(status_code=404, detail="Receipt not found")
    
    return {
        "id": updated_receipt["id"],
        "url": updated_receipt["url"],
        "name": updated_receipt["name"],
        "date": updated_receipt["date"],
        "tripId": updated_receipt["trip_id"]
    }

@app.delete("/api/receipts/{receipt_id}")
async def delete_receipt(receipt_id: str):
    receipt = await db_manager.get_receipt_by_id(receipt_id)
    if not receipt:
        raise HTTPException(status_code=404, detail="Receipt not found")
    
    # Delete file from filesystem
    file_path = f"uploads/receipts/{receipt['url'].split('/')[-1]}"
    if os.path.exists(file_path):
        os.remove(file_path)
    
    # Delete from database
    success = await db_manager.delete_receipt(receipt_id)
    if not success:
        raise HTTPException(status_code=404, detail="Receipt not found")
    
    return {"message": "Receipt deleted successfully"}

@app.get("/api/receipts/trip/{trip_id}", response_model=List[Receipt])
async def get_receipts_for_trip(trip_id: int):
    receipts_data = await db_manager.get_receipts_for_trip(trip_id)
    receipts = []
    for receipt in receipts_data:
        receipts.append({
            "id": receipt["id"],
            "url": receipt["url"],
            "name": receipt["name"],
            "date": receipt["date"],
            "tripId": receipt["trip_id"]
        })
    return receipts

# Location endpoints
@app.get("/api/locations", response_model=List[Location])
async def get_locations():
    locations_data = await db_manager.get_recent_locations(10)
    locations = []
    for location in locations_data:
        locations.append({
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "timestamp": location["timestamp"]
        })
    return locations

@app.post("/api/locations", response_model=Location)
async def add_location(location: Location):
    created_location = await db_manager.add_location(location.dict())
    return {
        "latitude": created_location["latitude"],
        "longitude": created_location["longitude"],
        "timestamp": created_location["timestamp"]
    }

# Trip route data
@app.get("/api/trips/{trip_id}/route")
async def get_trip_route(trip_id: int):
    route_points = await db_manager.get_trip_route(trip_id)
    if not route_points:
        raise HTTPException(status_code=404, detail="Route not found")
    
    route = []
    for point in route_points:
        route.append({
            "latitude": point["latitude"],
            "longitude": point["longitude"]
        })
    
    return {"route": route}

@app.post("/api/trips/{trip_id}/route")
async def add_trip_route(trip_id: int, route_points: List[dict]):
    # Verify trip exists
    trip = await db_manager.get_trip_by_id(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    
    await db_manager.add_trip_route_points(trip_id, route_points)
    return {"message": "Route points added successfully"}

# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Database management endpoints (for development)
@app.post("/api/admin/reset-database")
async def reset_database():
    """Reset database with sample data (development only)"""
    # This should be protected in production
    await db_manager.insert_sample_data()
    return {"message": "Database reset successfully"}

@app.get("/api/admin/database-info")
async def get_database_info():
    """Get database information (development only)"""
    try:
        # Get table information
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        tables = await db_manager.database.fetch_all(query)
        
        info = {"tables": []}
        for table in tables:
            table_name = table[0]
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count = await db_manager.database.fetch_val(count_query)
            info["tables"].append({"name": table_name, "count": count})
        
        return info
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
