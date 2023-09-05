from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

engine = create_engine("postgresql://postgres:dev@localhost:5432/postgres")

Base = automap_base()
Base.prepare(engine, reflect=True)

Storm = Base.classes.storms
Mission = Base.classes.missions
HighDensityObservation = Base.classes.high_density_observations
Observation = Base.classes.observations

session = Session(engine)
