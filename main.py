from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient("localhost", 27017)

db = client['Bookstore']  # Select the 'Bookstore' database

people = db['people']  # Select the 'people' collection

books = db['books']  # Select the 'books' collection

# Insert a document into the 'people' collection
people.insert_one({"name": "Arun", "age": 30})

# Insert a document into the 'people' collection and get the inserted ID
movie_id = people.insert_one({
    "title": "Dangal",
    "director": "Nitesh Tiwari",
    "year": 2016,
    "genres": ["Biography", "Drama", "Sport"],
    "rating": 8.4,
    "duration": 161
}).inserted_id

print(f"Movie id is {movie_id}")

# Insert multiple documents into the 'books' collection
books.insert_many([
    {
        "title": "3 Idiots",
        "director": "Rajkumar Hirani",
        "year": 2009,
        "genres": ["Comedy", "Drama"],
        "rating": 8.4,
        "duration": 170
    },
    {
        "title": "Dangal",
        "director": "Nitesh Tiwari",
        "year": 2016,
        "genres": ["Biography", "Drama", "Sport"],
        "rating": 8.4,
        "duration": 161
    },
    {
        "title": "Baahubali: The Beginning",
        "director": "S. S. Rajamouli",
        "year": 2015,
        "genres": ["Action", "Drama", "Fantasy"],
        "rating": 8.0,
        "duration": 159
    },
    {
        "title": "Piku",
        "director": "Shoojit Sircar",
        "year": 2015,
        "genres": ["Comedy", "Drama"],
        "rating": 7.6,
        "duration": 123
    },
    {
        "title": "Gully Boy",
        "director": "Zoya Akhtar",
        "year": 2019,
        "genres": ["Drama", "Music"],
        "rating": 8.0,
        "duration": 154
    }
])

books_with_low_rating = [p for p in books.find({"rating": {"$lt": 8}})]
print(books_with_low_rating)
print()
print("**************************************************")
print('\n')

count_drama = books.count_documents({"genres": "Drama"})
print(f"Number of books with genre 'Drama': {count_drama}")
print()
print("**************************************************")
print('\n')

update_result = books.update_many({"genres": "Drama"}, {"$set": {"genres": "drama"}})
print(f"Number of documents updated: {update_result.modified_count}")
print()
print("**************************************************")
print('\n')

all_books = [book for book in books.find()]
print(all_books)
print()
print("**************************************************")
print('\n')


#deleted_result=books.delete_many({"rating":{"$lt":8}})
#print(f'Delete the rating less than 7:{deleted_result}')

pipeline = [
    {"$group": {"_id": "$director", "count": {"$sum": 1}}}
]

result = list(books.aggregate(pipeline))
print(f"Unwind Genres and Group by Genre: {result}")
print()
print("**************************************************")
print('\n')

pipeline = [
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "averageRating": {"$avg": "$rating"}}}
]

result = list(books.aggregate(pipeline))
print(f"Average Rating by Genre: {result}")
print()
print("**************************************************")
print('\n')

pipeline = [
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "books": {"$push": "$title"}}}
]

result = list(books.aggregate(pipeline))
print(f"List of Books by Genre: {result}")
print()
print("**************************************************")
print('\n')

pipeline = [
    {"$unwind": "$genres"},
    {"$match": {"rating": {"$gt": 8}}},
    {"$group": {"_id": "$genres", "count": {"$sum": 1}}}
]

result = list(books.aggregate(pipeline))
print(f"Unwind Genres and Filter by Rating: {result}")
print()
print("**************************************************")
print('\n')


pipeline = [
    {"$unwind": "$genres"},
    {"$unwind": "$actors"},
    {"$group": {"_id": {"genre": "$genres", "actor": "$actors"}, "count": {"$sum": 1}}}
]

result = list(books.aggregate(pipeline))
print(f"Unwind Nested Arrays {result}")
print()
print("**************************************************")
print('\n')


