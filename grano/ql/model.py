from grano.core import db


class BidiRelation(db.Model):
    __tablename__ = 'grano_relation_bidi'

    id = db.Column(db.Unicode, primary_key=True)
    created_at = db.Column(db.DateTime)
    updated_at = db.Column(db.DateTime)
    reverse = db.Column(db.Boolean)
    
    relation_id = db.Column(db.Unicode)
    source_id = db.Column(db.Unicode)
    target_id = db.Column(db.Unicode)
    project_id = db.Column(db.Integer)
    schema_id = db.Column(db.Integer)
    author_id = db.Column(db.Integer)
