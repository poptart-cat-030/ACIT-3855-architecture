from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, Float, String, DateTime, func

class Base(DeclarativeBase):
    pass

class Volume(Base):
    __tablename__ = "Volume"
    id = mapped_column(Integer, primary_key=True)
    # Data provided by client
    salon_id = mapped_column(String(250), nullable=False)
    salon_name = mapped_column(String(250), nullable=False)
    hair_volume = mapped_column(Float, nullable=False)
    disposal_method = mapped_column(String(250), nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    reading_timestamp = mapped_column(DateTime, nullable=False)
    # Date created by us
    date_created = mapped_column(DateTime, nullable=False, default=func.now())
    trace_id = mapped_column(String(250), nullable=False)

    # Helper methods for putting object attributes into python dict
    # Dictionary representation of a hair volume reading
    def to_dict(self):
        dict = {}
        # dict['id'] = self.id
        dict['salon_id'] = self.salon_id
        dict['salon_name'] = self.salon_name
        dict['hair_volume'] = self.hair_volume
        dict['disposal_method'] = self.disposal_method
        dict['batch_timestamp'] = self.batch_timestamp
        dict['reading_timestamp'] = self.reading_timestamp
        # dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id

        return dict


class Type(Base):
    __tablename__ = "Type"
    id = mapped_column(Integer, primary_key=True)
    # Data provided by client
    salon_id = mapped_column(String(250), nullable=False)
    salon_name = mapped_column(String(250), nullable=False)
    hair_colour = mapped_column(String(250), nullable=False)
    hair_texture = mapped_column(String(250), nullable=False)
    hair_thickness = mapped_column(Float, nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    reading_timestamp = mapped_column(DateTime, nullable=False)
    # Date created by us
    date_created = mapped_column(DateTime, nullable=False, default=func.now())
    trace_id = mapped_column(String(250), nullable=False)

    # Helper methods for putting object attributes into python dict
    # Dictionary representation of a hair type reading
    def to_dict(self):
        dict = {}
        # dict['id'] = self.id
        dict['salon_id'] = self.salon_id
        dict['salon_name'] = self.salon_name
        dict['hair_colour'] = self.hair_colour
        dict['hair_texture'] = self.hair_texture
        dict['hair_thickness'] = self.hair_thickness
        dict['batch_timestamp'] = self.batch_timestamp
        dict['reading_timestamp'] = self.reading_timestamp
        # dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id
        
        return dict