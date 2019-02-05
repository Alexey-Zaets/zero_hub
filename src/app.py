
import os
import requests
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import desc
from flask_migrate import Migrate
from celery import Celery


load_dotenv(find_dotenv())

app = Flask(__name__)

# Config
app.config.update(
    DEBUG=True,
    SQLALCHEMY_DATABASE_URI=os.getenv('SQLALCHEMY_DATABASE_URI'),
    CELERY_BROKER_URL='redis://localhost:6379',
    CELERY_RESULT_BACKEND='redis://localhost:6379',
    CELERYD_CONCURRENCY=4
)

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# DB config
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# DB model
class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.String(120), unique=True, nullable=False)
    url = db.Column(db.String(120), nullable=False)
    task_status = db.Column(db.String(60), nullable=True)
    response_body = db.Column(db.Text, nullable=True)
    response_content_lenght = db.Column(db.Integer, nullable=False)
    response_http_status = db.Column(db.Integer, nullable=False)

    def check_status(self):
        task = open_url.AsyncResult(self.task_id)
        if task.status == 'PANDING':
            self.task_status = 'New'
        elif task.status == 'STARTED':
            self.task_status = 'Pending'
        elif task.status == 'SUCCESS':
            self.task_status = 'Completed'
        elif task.status == 'FAILURE':
            self.task_status = 'Error'
        db.session.add(self)
        db.session.commit()
        return self

    def to_dict(self):
        return {
            'id': self.task_id,
            'url': self.url,
            'status': self.task_status,
            'response_content_lenght': self.response_content_lenght,
            'response_body': self.response_body,
            'response_http_status': self.response_http_status
        }


# Celery task
@celery.task(bind=True)
def open_url(self, url):
    url = url if url.startswith('http') else 'http://' + url
    response = requests.get(url, allow_redirects=True)
    http_code = response.status_code
    response_body = response.text if http_code == 200 else None
    response_content_lenght = len(response_body) if http_code == 200 else 0
    result_dict = {
        'id': self.request.id,
        'url': url,
        'response_content_lenght': response_content_lenght,
        'response_http_status': http_code,
        'response_body': response_body,
    }
    self.update_state(meta=result_dict)
    obj = Task(
        task_id=self.request.id,
        url=url,
        response_content_lenght=response_content_lenght,
        response_body=response_body,
        response_http_status=http_code
    )
    db.session.add(obj)
    db.session.commit()
    return result_dict

# Views
@app.route('/', methods=['GET'])
def result():
    task_id = request.args.get('id')
    if task_id is not None:
        task_from_db = Task.query.filter_by(task_id=task_id).first_or_404()
        return jsonify(task_from_db.check_status().to_dict())
    else:
        last_ten_tasks = Task.query.order_by(desc(Task.id)).all()[:10]
        last_ten_tasks = [i.check_status() for i in last_ten_tasks]
        return jsonify([i.to_dict() for i in last_ten_tasks])

@app.route('/', methods=['POST'])
def send():
    if request.is_json:
        url = request.get_json().get('url')
        if url is not None:
            task_id = open_url.delay(url)
    return jsonify(id=str(task_id))


if __name__ == '__main__':
    app.run()
