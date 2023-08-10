import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import datetime as dt

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

#Flask Setup
app = Flask(__name__)

#Define Flask Routes
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
       
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    # Find the most recent date in the data set.
    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # Get the last 12 months date frm the recent date
    last_year = dt.datetime.strptime(recent_date[0],'%Y-%m-%d').date() - dt.timedelta(days=365)

    # Query the prcp results and the date
    precipitation = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date>=last_year).all()
    # Create a dataframe with date and precipitation column, store value from the query result and remove any row with NA result.
    precip_df = pd.DataFrame(precipitation, columns=['Date', 'Precipitation']).set_index('Date').dropna()
    
    # Convert dataframe to dictionary
    precip_dict = pd.Series(precip_df.Precipitation.values, index=precip_df.Date).to_dict()

    # Close the session
    session.close()

    #Return the JSON representation from the dictionary
    return jsonify(precip_dict)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    # Query the data for all stations
    stations = session.query(Station.id, Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()

    # Create a dataframe with date and precipitation column, store value from the query result and remove any row with NA result.
    stations_df = pd.DataFrame(stations, columns=['ID', 'Station','Name','Latitude','Longitude','Elevation'])
    
    # Convert dataframe to dictionary
    stations_dict = stations_df.set_index('ID').T.to_dict('list')

    # Close the session
    session.close()

    #Return the JSON representation from the dictionary
    return jsonify(stations_dict)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    # Get the most active station name
    most_active = session.query(Measurement.station).group_by(Measurement.station).\
    order_by(func.count(Measurement.station).desc()).first()

    # Get the recent date from the most active station id
    ma_recent_date = session.query(Measurement.date).filter(Measurement.station == most_active[0]).order_by(Measurement.date.desc()).first()
    # Get the last 12 months date frm the recent date
    most_active_year = dt.datetime.strptime(ma_recent_date[0],'%Y-%m-%d').date() - dt.timedelta(days=365)

    # Query the dates and temperature observations of the most-active station for the previous year
    tobs_result = session.query(Measurement.tobs).filter(Measurement.date>=most_active_year, Measurement.station == most_active[0]).all()

    # Create a dataframe with date and precipitation column, store value from the query result and remove any row with NA result.
    tobs_df = pd.DataFrame(tobs_result, columns=['Date', 'Temperature'])
    
    # Convert dataframe to dictionary
    tobs_dict = pd.Series(tobs_df.Temperature.values, index=tobs_df.Date).to_dict()

    # Close the session
    session.close()

    #Return the JSON representation from the dictionary
    return jsonify(tobs_dict)

@app.route("/api/v1.0/<start>")
def start(start):
    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    # Query the dates and temperature observations of the most-active station for the previous year
    prev_tobs_result = session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).all()

    # Store each min, avg and max into the dictionary
    for min, avg, max in prev_tobs_result:
        prev_tobs = {}
        prev_tobs["min"] = min
        prev_tobs["average"] = avg
        prev_tobs["max"] = max

    # Close the session
    session.close()

    #Return the JSON representation from the dictionary
    return jsonify(prev_tobs)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    # Query the dates and temperature observations of the most-active station for the previous year
    most_active_prev_tobs = session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
        filter(Measurement.date >= start, Measurement.date <= end).all()

    # Store each min, avg and max into the dictionary
    for min, avg, max in most_active_prev_tobs:
        ma_prev_tobs = {}
        ma_prev_tobs["min"] = min
        ma_prev_tobs["average"] = avg
        ma_prev_tobs["max"] = max

    # Close the session
    session.close()

    #Return the JSON representation from the dictionary
    return jsonify(ma_prev_tobs)

if __name__ == '__main__':
    app.run(debug=True)