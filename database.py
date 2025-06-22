import os
import asyncio
import asyncpg
from datetime import datetime
from typing import List, Dict, Any, Optional
from databases import Database
import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid

# Database URL from environment variable or default to your Neon connection
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://miletracker_backend_owner:npg_4T5tcZnqKYrG@ep-still-waterfall-a5y97u5h-pooler.us-east-2.aws.neon.tech/miletracker_backend?sslmode=require"
)

# Create database instance
database = Database(DATABASE_URL)

# SQLAlchemy metadata
metadata = MetaData()

# Define tables
users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email", String(255), unique=True, nullable=False),
    Column("password_hash", String(255), nullable=False),
    Column("first_name", String(100)),
    Column("last_name", String(100)),
    Column("is_active", Boolean, default=True),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)

trips_table = Table(
    "trips",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("date", String(50), nullable=False),
    Column("start_time", String(50), nullable=False),
    Column("end_time", String(50), nullable=False),
    Column("start_location", String(255), nullable=False),
    Column("end_location", String(255), nullable=False),
    Column("distance", Float, nullable=False),
    Column("potential", Float, nullable=False),
    Column("type", String(20), nullable=False),  # 'personal' or 'business'
    Column("notes", Text, default=""),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)

receipts_table = Table(
    "receipts",
    metadata,
    Column("id", String(36), primary_key=True),  # UUID string
    Column("url", String(500), nullable=False),
    Column("name", String(255), nullable=False),
    Column("date", DateTime, nullable=False),
    Column("trip_id", Integer, ForeignKey("trips.id", ondelete="SET NULL")),
    Column("file_size", Integer),
    Column("mime_type", String(100)),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)

locations_table = Table(
    "locations",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("latitude", Float, nullable=False),
    Column("longitude", Float, nullable=False),
    Column("timestamp", sa.BigInteger, nullable=False),  # Unix timestamp in milliseconds
    Column("accuracy", Float),
    Column("altitude", Float),
    Column("speed", Float),
    Column("created_at", DateTime, default=datetime.utcnow),
)

trip_routes_table = Table(
    "trip_routes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("trip_id", Integer, ForeignKey("trips.id", ondelete="CASCADE"), nullable=False),
    Column("latitude", Float, nullable=False),
    Column("longitude", Float, nullable=False),
    Column("timestamp", sa.BigInteger),
    Column("sequence_order", Integer, nullable=False),
    Column("created_at", DateTime, default=datetime.utcnow),
)

trip_stats_table = Table(
    "trip_stats",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("total_drives", Integer, default=0),
    Column("total_miles", Float, default=0),
    Column("total_logged", Float, default=0),
    Column("business_miles", Float, default=0),
    Column("personal_miles", Float, default=0),
    Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)

class DatabaseManager:
    def __init__(self):
        self.database = database
    
    async def connect(self):
        """Connect to the database"""
        await self.database.connect()
    
    async def disconnect(self):
        """Disconnect from the database"""
        await self.database.disconnect()
    
    async def create_tables(self):
        """Create all tables if they don't exist"""
        # Create engine for table creation
        engine = sa.create_engine(DATABASE_URL)
        metadata.create_all(engine)
        engine.dispose()
        
        # Insert sample data if tables are empty
        await self.insert_sample_data()
    
    async def insert_sample_data(self):
        """Insert sample data if database is empty"""
        # Check if we already have data
        query = "SELECT COUNT(*) FROM trips"
        count = await self.database.fetch_val(query)
        
        if count > 0:
            return
        
        # Insert sample trips
        sample_trips = [
            {
                'date': 'WED 27',
                'start_time': '3:20 PM',
                'end_time': '4:05 PM',
                'start_location': 'Home',
                'end_location': 'Work',
                'distance': 2.5,
                'potential': 1.34,
                'type': 'business',
                'notes': ''
            },
            {
                'date': 'THU 28',
                'start_time': '1:15 PM',
                'end_time': '2:10 PM',
                'start_location': 'Work',
                'end_location': 'Client Meeting',
                'distance': 3.1,
                'potential': 1.66,
                'type': 'business',
                'notes': ''
            }
        ]
        
        trip_ids = []
        for trip in sample_trips:
            query = trips_table.insert().values(**trip)
            trip_id = await self.database.execute(query)
            trip_ids.append(trip_id)
        
        # Insert sample route data
        sample_route_points = [
            {"latitude": 28.3289978, "longitude": -81.4928141},
            {"latitude": 28.3279107, "longitude": -81.4928196},
            {"latitude": 28.3294731, "longitude": -81.4929894},
            {"latitude": 28.3292194, "longitude": -81.4939528},
            {"latitude": 28.3291996, "longitude": -81.4945516},
            {"latitude": 28.3291808, "longitude": -81.4949539},
            {"latitude": 28.3293274, "longitude": -81.495393},
            {"latitude": 28.3293372, "longitude": -81.495501},
            {"latitude": 28.3300992, "longitude": -81.4952811},
            {"latitude": 28.3307153, "longitude": -81.4953258},
            {"latitude": 28.3315591, "longitude": -81.495316}
        ]
        
        # Add route points for both trips
        for trip_id in trip_ids:
            for i, point in enumerate(sample_route_points):
                query = trip_routes_table.insert().values(
                    trip_id=trip_id,
                    latitude=point['latitude'],
                    longitude=point['longitude'],
                    sequence_order=i
                )
                await self.database.execute(query)
        
        # Initialize stats
        query = trip_stats_table.insert().values(
            id=1,
            total_drives=89,
            total_miles=732,
            total_logged=385
        )
        await self.database.execute(query)
    
    # Trip operations
    async def get_all_trips(self) -> List[Dict[str, Any]]:
        query = """
            SELECT id, date, start_time, end_time, start_location, end_location,
                   distance, potential, type, notes, created_at, updated_at
            FROM trips ORDER BY id DESC
        """
        rows = await self.database.fetch_all(query)
        return [dict(row) for row in rows]
    
    async def get_trip_by_id(self, trip_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT id, date, start_time, end_time, start_location, end_location,
                   distance, potential, type, notes, created_at, updated_at
            FROM trips WHERE id = :trip_id
        """
        row = await self.database.fetch_one(query, {"trip_id": trip_id})
        return dict(row) if row else None
    
    async def update_trip(self, trip_id: int, **kwargs) -> Optional[Dict[str, Any]]:
        if not kwargs:
            return await self.get_trip_by_id(trip_id)
        
        # Build dynamic update query
        set_clauses = []
        values = {"trip_id": trip_id}
        
        for key, value in kwargs.items():
            if key in ['type', 'notes']:  # Only allow certain fields to be updated
                set_clauses.append(f"{key} = :{key}")
                values[key] = value
        
        if not set_clauses:
            return await self.get_trip_by_id(trip_id)
        
        set_clauses.append("updated_at = NOW()")
        
        query = f"""
            UPDATE trips SET {", ".join(set_clauses)}
            WHERE id = :trip_id
        """
        await self.database.execute(query, values)
        
        return await self.get_trip_by_id(trip_id)
    
    async def create_trip(self, trip_data: Dict[str, Any]) -> Dict[str, Any]:
        query = trips_table.insert().values(**trip_data)
        trip_id = await self.database.execute(query)
        return await self.get_trip_by_id(trip_id)
    
    async def delete_trip(self, trip_id: int) -> bool:
        query = "DELETE FROM trips WHERE id = :trip_id"
        result = await self.database.execute(query, {"trip_id": trip_id})
        return result > 0
    
    # Receipt operations
    async def get_all_receipts(self) -> List[Dict[str, Any]]:
        query = """
            SELECT id, url, name, date, trip_id, file_size, mime_type, created_at, updated_at
            FROM receipts ORDER BY created_at DESC
        """
        rows = await self.database.fetch_all(query)
        return [dict(row) for row in rows]
    
    async def get_receipts_for_trip(self, trip_id: int) -> List[Dict[str, Any]]:
        query = """
            SELECT id, url, name, date, trip_id, file_size, mime_type, created_at, updated_at
            FROM receipts WHERE trip_id = :trip_id ORDER BY created_at DESC
        """
        rows = await self.database.fetch_all(query, {"trip_id": trip_id})
        return [dict(row) for row in rows]
    
    async def create_receipt(self, receipt_data: Dict[str, Any]) -> Dict[str, Any]:
        query = receipts_table.insert().values(**receipt_data)
        await self.database.execute(query)
        return await self.get_receipt_by_id(receipt_data['id'])
    
    async def get_receipt_by_id(self, receipt_id: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT id, url, name, date, trip_id, file_size, mime_type, created_at, updated_at
            FROM receipts WHERE id = :receipt_id
        """
        row = await self.database.fetch_one(query, {"receipt_id": receipt_id})
        return dict(row) if row else None
    
    async def update_receipt_trip(self, receipt_id: str, trip_id: Optional[int]) -> Optional[Dict[str, Any]]:
        query = """
            UPDATE receipts SET trip_id = :trip_id, updated_at = NOW()
            WHERE id = :receipt_id
        """
        await self.database.execute(query, {"trip_id": trip_id, "receipt_id": receipt_id})
        return await self.get_receipt_by_id(receipt_id)
    
    async def delete_receipt(self, receipt_id: str) -> bool:
        query = "DELETE FROM receipts WHERE id = :receipt_id"
        result = await self.database.execute(query, {"receipt_id": receipt_id})
        return result > 0
    
    # Location operations
    async def get_recent_locations(self, limit: int = 10) -> List[Dict[str, Any]]:
        query = """
            SELECT id, latitude, longitude, timestamp, accuracy, altitude, speed, created_at
            FROM locations ORDER BY timestamp DESC LIMIT :limit
        """
        rows = await self.database.fetch_all(query, {"limit": limit})
        return [dict(row) for row in rows]
    
    async def add_location(self, location_data: Dict[str, Any]) -> Dict[str, Any]:
        query = locations_table.insert().values(**location_data)
        location_id = await self.database.execute(query)
        
        get_query = """
            SELECT id, latitude, longitude, timestamp, accuracy, altitude, speed, created_at
            FROM locations WHERE id = :location_id
        """
        row = await self.database.fetch_one(get_query, {"location_id": location_id})
        return dict(row)
    
    # Trip route operations
    async def get_trip_route(self, trip_id: int) -> List[Dict[str, Any]]:
        query = """
            SELECT latitude, longitude, timestamp, sequence_order
            FROM trip_routes WHERE trip_id = :trip_id ORDER BY sequence_order
        """
        rows = await self.database.fetch_all(query, {"trip_id": trip_id})
        return [dict(row) for row in rows]
    
    async def add_trip_route_points(self, trip_id: int, points: List[Dict[str, Any]]):
        for i, point in enumerate(points):
            query = trip_routes_table.insert().values(
                trip_id=trip_id,
                latitude=point['latitude'],
                longitude=point['longitude'],
                timestamp=point.get('timestamp'),
                sequence_order=i
            )
            await self.database.execute(query)
    
    # Statistics operations
    async def get_trip_stats(self) -> Dict[str, int]:
        query = """
            SELECT total_drives, total_miles, total_logged, business_miles, personal_miles
            FROM trip_stats WHERE id = 1
        """
        row = await self.database.fetch_one(query)
        if row:
            return dict(row)
        else:
            # Calculate stats from actual data
            return await self.calculate_trip_stats()
    
    async def calculate_trip_stats(self) -> Dict[str, int]:
        query = """
            SELECT COUNT(*) as total_drives, 
                   COALESCE(SUM(distance), 0) as total_miles,
                   COALESCE(SUM(potential), 0) as total_logged,
                   COALESCE(SUM(CASE WHEN type = 'business' THEN distance ELSE 0 END), 0) as business_miles,
                   COALESCE(SUM(CASE WHEN type = 'personal' THEN distance ELSE 0 END), 0) as personal_miles
            FROM trips
        """
        row = await self.database.fetch_one(query)
        stats = dict(row)
        
        # Update cached stats
        upsert_query = """
            INSERT INTO trip_stats (id, total_drives, total_miles, total_logged, business_miles, personal_miles, updated_at)
            VALUES (1, :total_drives, :total_miles, :total_logged, :business_miles, :personal_miles, NOW())
            ON CONFLICT (id) DO UPDATE SET
                total_drives = EXCLUDED.total_drives,
                total_miles = EXCLUDED.total_miles,
                total_logged = EXCLUDED.total_logged,
                business_miles = EXCLUDED.business_miles,
                personal_miles = EXCLUDED.personal_miles,
                updated_at = NOW()
        """
        await self.database.execute(upsert_query, stats)
        
        return stats
    
    async def update_trip_stats(self):
        """Recalculate and update trip statistics"""
        return await self.calculate_trip_stats()

# Global database manager instance
db_manager = DatabaseManager()
