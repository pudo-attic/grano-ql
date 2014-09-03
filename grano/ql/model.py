from grano.core import db
from grano.model.common import UUIDBase
from grano.model.property import Property, PropertyBase


class BidiRelation(db.Model, PropertyBase):
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

    properties = db.relationship(Property,
                                 order_by=Property.created_at.desc(),
                                 viewonly=True,
                                 foreign_keys=id,
                                 primaryjoin='BidiRelation.id==Property.relation_id')

