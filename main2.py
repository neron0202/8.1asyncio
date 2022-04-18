import aiohttp
import requests
import asyncio
import time
from more_itertools import chunked

import sqlalchemy
import psycopg2

con = psycopg2.connect(database='star_wars', user='postgres', password='1968', host='localhost')
cur = con.cursor()


async def get_person(session: aiohttp.ClientSession, person_id: int) -> dict:
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as resp:
        return await resp.json()


async def main():
    start = time.time()
    con = psycopg2.connect(database='star_wars', user='postgres', password='1968', host='localhost')
    cur = con.cursor()
    print("Подключение к БД осуществлено")
    async with aiohttp.ClientSession() as session:
        for chunk in chunked(range(1, 10), 10):
            print(chunk)
            person_coroutine_list = []
            for i in chunk:
                person_coroutine = get_person(session, i)
                print('person_coroutine=', person_coroutine)
                person_coroutine_list.append(person_coroutine)
            persons = await asyncio.gather(*person_coroutine_list)
            for person in persons:
                print('person=', person)
                # id = person['id']
                birth_year = person['birth_year']
                eye_color = person['eye_color']
                films = str(person['films'])
                gender = person['gender']
                hair_color = person['hair_color']
                height = person['height']
                homeworld = person['homeworld']
                mass = person['mass']
                name = person['name']
                skin_color = person['skin_color']
                species = str(person['species'])
                starships = str(person['starships'])
                vehicles = str(person['vehicles'])
                cur.execute(f'INSERT INTO sw_heroes(birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles))
            con.commit()
            cur.close()
            con.close()
        print(persons)
    print("Время работы: ", time.time() - start)

asyncio.run(main())
