from data_collector import createdb, mapper
from dashboard import create_dashboard


engine = createdb.create_db('hospitals', 'newuser', 'newPassword')

mapper.map_hospitals(engine)

app = create_dashboard()

# Run the server
if __name__ == "__main__":
    app.run_server(debug=True)