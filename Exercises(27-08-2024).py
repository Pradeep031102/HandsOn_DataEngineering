# Exercise 1: Create a Dictionary
person = {
    "Name": "Alice",
    "Age": 25,
    "City": "New York"
}
print(person)

# Exercise 2: Access Dictionary Elements
print(person["Name"])
print(person["City"])

# Exercise 3: Add and Modify Elements
person["email"] = "alice@example.com"
person["Age"] = 26
print(person)

# Exercise 4: Remove Elements
del person["City"]
print(person)

# Exercise 5: Check if a Key Exists
print("email exists" if "email" in person else "email does not exist")
print("phone exists" if "phone" in person else "phone does not exist")

# Exercise 6: Loop Through a Dictionary
for key, value in person.items():
    print(f"{key}: {value}")

for key in person.keys():
    print(key)

for value in person.values():
    print(value)

# Exercise 7: Nested Dictionary
employees = {
    1: {"name": "Bob", "job": "Engineer"},
    2: {"name": "Sue", "job": "Designer"},
    3: {"name": "Tom", "job": "Manager"}
}
print(employees[2])
employees[4] = {"name": "Linda", "job": "HR"}
print(employees)

# Exercise 8: Dictionary Comprehension
squares = {x: x**2 for x in range(1, 6)}
print(squares)

# Exercise 9: Merge Two Dictionaries
dict1 = {"a": 1, "b": 2}
dict2 = {"c": 3, "d": 4}
dict1.update(dict2)
print(dict1)

# Exercise 10: Default Dictionary Values
letter_to_number = {"a": 1, "b": 2, "c": 3}
print(letter_to_number.get("b"))
print(letter_to_number.get("d", 0))

# Exercise 11: Dictionary from Two Lists
keys = ["name", "age", "city"]
values = ["Eve", 29, "San Francisco"]
dictionary = dict(zip(keys, values))
print(dictionary)

# Exercise 12: Count Occurrences of Words
sentence = "the quick brown fox jumps over the lazy dog the fox"
word_count = {}
for word in sentence.split():
    word_count[word] = word_count.get(word, 0) + 1
print(word_count)