from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import models

engine = create_engine('sqlite:///gb_blog.db')

models.Base.metadata.create_all(bind=engine)

session_maker = sessionmaker(bind=engine)

if __name__ == '__main__':
    db = session_maker()
    print(1)
