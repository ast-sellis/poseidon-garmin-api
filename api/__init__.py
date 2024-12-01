import os
from flask import Flask
from werkzeug.middleware.profiler import ProfilerMiddleware

profiler_dir = "./profiler"
os.makedirs(profiler_dir, exist_ok=True)

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )
    app.wsgi_app = ProfilerMiddleware(
        app.wsgi_app,
        profile_dir=profiler_dir,  # Directory to save profile files
        restrictions=[30],        # Limit to top 30 slowest functions
    )



    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    from .resources.garmin_activity import garmin_bp
    from .resources.garmin_geojson import garmin_geojson_bp

    app.register_blueprint(garmin_bp)
    app.register_blueprint(garmin_geojson_bp)
    
    return app