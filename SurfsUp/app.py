import numpy as np

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify
import datetime as dt


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
m_tbl = Base.classes.measurement
s_tbl = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>"
        f"/api/v1.0/start_date/end_date"
    )

###########################################################################
# Go back by 12 months from the latest record to get the end date range
###########################################################################
def getLastYearEndDate(session) :
    # Create our session (link) from Python to the DB
    
    last_date = session.query(m_tbl.date).\
        order_by(m_tbl.date.desc()).\
            first()
    last_date_str = last_date.date
    last_datetime = dt.datetime.fromisoformat(last_date_str)

    # Calculate the date one year from the last date in data set.
    #Substract 365 days to go back by an year
    query_datetime = last_datetime - dt.timedelta(days=365)
    return query_datetime
    
#################################################
# Handler for the precipitation route
#################################################
@app.route("/api/v1.0/precipitation")
def precipitationAnalysis():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    query_datetime = getLastYearEndDate(session)
    
    results = session.query(m_tbl.date,m_tbl.prcp).\
        filter( m_tbl.date >= query_datetime.date()).\
            all()
    
    session.close()

    precip = {date: prcp for date, prcp in results}
    return jsonify(precip)


#################################################
# Handler for the stations route
#################################################
@app.route("/api/v1.0/stations")
def getStations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    sel = [m_tbl.station]

    station_grp_rslt = session.query(*sel).\
        group_by(m_tbl.station).\
            all()
    
    session.close()

    stations = list(np.ravel(station_grp_rslt))
    
    return jsonify(stations)

#################################################
# Handler for the tobs route
#################################################
@app.route("/api/v1.0/tobs")
def mostActiveStationTempData():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # pylint: disable=E1102
    sel = [m_tbl.station,func.count(m_tbl.station)]
    
    station_grp_query = session.query(*sel).\
        group_by(m_tbl.station).\
            order_by(func.count(m_tbl.station).desc()).\
                all()
    
    most_active_st_name = station_grp_query[0].station

    query_datetime = getLastYearEndDate(session)

    results = session.query(m_tbl.tobs).\
        filter(m_tbl.station == most_active_st_name, \
               m_tbl.date >= query_datetime).all()
    
    session.close()

    most_active_station_temp = list(np.ravel(results))
    
    return jsonify(most_active_station_temp)

################################################################
# Handler for the start date or start and end date route
################################################################  
@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def TempDetailsForDateRange(start= None, end = None):
    
    sel = [func.min(m_tbl.tobs),
      func.max(m_tbl.tobs),
      func.avg(m_tbl.tobs)]
    
    session = Session(engine)

    if not end:
        results = session.query(*sel).\
        filter(m_tbl.date >= start).all()   
    else:
        results = session.query(*sel).\
        filter(m_tbl.date >= start, m_tbl.date <= end).all()  
    
    session.close()
    
    temp_info = list(np.ravel(results))
    return jsonify(temp_info)

if __name__ == '__main__':
    app.run(debug=True)
