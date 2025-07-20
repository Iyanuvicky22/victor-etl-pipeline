"""
Goal: Load cleaned and transformed IMDB data into a
      database named imdb_db

Author: Arowosegbe Victor Iyanuoluwa\n
Email: Iyanuvicky@gmail.com\n
Github: https://github.com/Iyanuvicky22
"""
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, create_engine, ForeignKey
from sqlalchemy import Integer, String, DECIMAL
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.exc import SQLAlchemyError
from data_transform import transform_df

load_dotenv(dotenv_path='.env')

DB_URL = os.getenv('DB_URL')

Base = declarative_base()


def connect_db():
    """
    Setting up database connection.
    """
    try:
        engine = create_engine(DB_URL, echo=True)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        # session = Session()
        logging.info('\n\nCongratulations!!! Database successfully connected to.\n\n')
        return Session, engine
    except SQLAlchemyError as e:
        logging.error(f'Connection Error {e}')
        return None, None
    

class Movies(Base):
    """
    Movies Table definition class
    Args:
        Base (Class): Table declarative class
    """
    __tablename__ = 'movies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200),unique=True)
    description = Column(String, )
    release_year = Column(String(4))
    movie_type = Column(String(40))
    movies_id = relationship('Details', backref='movies')


class Details(Base):
    """
    Details of movies in the database
    Args:
        Base (Class): Table declarative class
    """
    __tablename__ = 'details'
    id = Column(Integer, primary_key=True)
    details_id = Column(Integer, ForeignKey('movies.id'))
    director = Column(String(100))
    duration = Column(DECIMAL(5, 2))
    ratings = Column(DECIMAL(2, 1))
    votes = Column(Integer)


class Sales(Base):
    """
    Sales info of movies
    Args:
        Base (Class): Table declarative class
    """
    __tablename__ = 'gross_sales'
    id = Column(Integer, primary_key=True)
    sales = Column(DECIMAL(20, 2))


def load_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Loading IMDb Data into the imdb_db

    """
    Session, engine = connect_db()
    # data = transform_df()

    with Session.begin() as session:
        for _, row in data.iterrows():
            movie = session.query(Movies).filter_by(
                title=row['title']
            ).first()
            if not movie:
                movie = Movies(
                    title=row['title'],
                    description=row['description'],
                    release_year=row['release_year'],
                    movie_type=row['type']
                )
                session.add(movie)

            details = Details(
                details_id=movie.id,
                director=row['director'],
                duration=row['runtime'],
                ratings=row['rating'],
                votes=row['votes_count']
            )
            session.add(details)

            sales = Sales(
                sales=row['total_sales']
            )
            session.add(sales)
        session.commit()


# if __name__ == '__main__':
#     load_data()

