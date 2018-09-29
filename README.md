# flask-tutorial
* Tutorial
    * Project Layout
    * Application Setup
      1. set FLASK_APP=flaskr
      2. set FLASK_ENV=development
      3. flask run
    * Define and Access the Database
      1. flask init-db
    * Blueprints and Views
    * Templates
    * Blog Blueprint
    * Make the Project Installable
      1. venv\Scripts\activate
      2. pip install -e .
      3. pip list
    * Test Coverage
      1. coverage run -m pytest
    * Deploy to Production
      1. pip install wheel
      2. python setup.py bdist_wheel
      3. pip install flaskr-1.0.0-py3-none-any.whl
      4. export FLASK_APP=flaskr
      5. flask init-db
    * Keep Developing!
* Templates
* Testing Flask Applications
  1. pip install pytest
* Application Errors
* Debugging Application Errors
 

# flask-restful
* Quickstart
* Request Parsing(marshmallow: deserializing objects(loading))
* Output Fields(marshmallow: serializing objects(dumping))
* Extending Flask-RESTful
* Intermediate Usage

# marshmallow
* Quickstart
* nesting schemas
* extending schemas
* examples

# flask-sqlalchemy
* Quickstart
* Introduction into Contexts(init_app)
* Configuration(SQLAlchemy configuration)
* Declaring Models
* Select, Insert, Delete
* Multiple Databases with Binds(SQLALCHEMY_BINDS, bind, __bind_key__)
* Signalling Support
* Customizing
  1. Model Class(db = SQLAlchemy(model_class=IdModel))
  2. Model Mixins(class Post(TimestampMixin, db.Model))
  3. Query Class(db = SQLAlchemy(query_class=GetOrQuery))
  4. Model Metaclass(db = SQLAlchemy(model_class=declarative_base(cls=Model, metaclass=CustomMeta, name='Model')))
  
 # flask-mail
* Quickstart

# requirement.txt
* pip freeze > requirements.txt
* pip install -r requirements.txt