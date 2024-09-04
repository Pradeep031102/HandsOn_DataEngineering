#1
numbers = [1, 2, 3, 4, 5]
numbers.append(6)
numbers.remove(3)
numbers.insert(0, 0)
print(numbers)

#2
coordinates = (10.0, 20.0, 30.0)
print(coordinates[1])

#3
fruits = {"apple", "banana", "cherry"}
fruits.add("orange")
fruits.remove("banana")
print("cherry is in fruits" if "cherry" in fruits else "cherry is not in fruits")
citrus = {"orange", "lemon", "lime"}
print(fruits.union(citrus))
print(fruits.intersection(citrus))

#4
person = {"name": "John", "age": 30, "city": "New York"}
print(person["name"])
person["age"] = 31
person["email"] = "john@example.com"
del person["city"]
print(person)

#5
school = {
    "Alice": {"Math": 90, "Science": 85},
    "Bob": {"Math": 78, "Science": 92},
    "Charlie": {"Math": 95, "Science": 88}
}
print(school["Alice"]["Math"])
school["David"] = {"Math": 80, "Science": 89}
school["Bob"]["Science"] = 95
print(school)

#6
numbers = [1, 2, 3, 4, 5]
squared_numbers = [x**2 for x in numbers]
print(squared_numbers)

#7
squared_set = {x**2 for x in [1, 2, 3, 4, 5]}
print(squared_set)

#8
cubed_dict = {x: x**3 for x in range(1, 6)}
print(cubed_dict)

#9
keys = ["name", "age", "city"]
values = ["Alice", 25, "Paris"]
combined_dict = dict(zip(keys, values))
print(combined_dict)

#10
sentence = "the quick brown fox jumps over the lazy dog the fox"
word_count = {}
for word in sentence.split():
    word_count[word] = word_count.get(word, 0) + 1
print(word_count)

#11
set1 = {1, 2, 3, 4, 5}
set2 = {4, 5, 6, 7, 8}
print(set1.union(set2))
print(set1.intersection(set2))
print(set1.difference(set2))

#12
tuple_data = ("Alice", 25, "Paris")
name, age, city = tuple_data
print(name, age, city)

#13
text = "hello world"
frequency = {}
for letter in text.replace(" ", ""):
    frequency[letter] = frequency.get(letter, 0) + 1
print(frequency)

#14
students = [("Alice", 90), ("Bob", 80), ("Charlie", 85)]
sorted_students = sorted(students, key=lambda x: x[1], reverse=True)
print(sorted_students)
