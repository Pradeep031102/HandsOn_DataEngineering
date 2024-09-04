###1)Create a List
fruits = ["apple", "banana", "cherry", "date", "berry"]
print(fruits)

###2)Access List Elements
print(fruits[0])
print(fruits[-1])
print(fruits[1])
print(fruits[3])

###3)Modify a List
fruits[1] = "blueberry"
print(fruits)

### 4)Add and Remove Elements

fruits.append("fig")
fruits.append("grape")
fruits.remove("apple")
print(fruits)

###5) Slice a List

first_three_fruits = fruits[:3]
print(first_three_fruits)


###6)Find List Length

print(len(fruits))


###7)List Concatenation
vegetables = ["carrot", "broccoli", "spinach"]
food = fruits + vegetables
print(food)


###8)Loop Through a List

for fruit in fruits:
    print(fruit)

###9)Check for Membership

print("cherry is in fruits" if "cherry" in fruits else "cherry is not in fruits")
print("mango is in fruits" if "mango" in fruits else "mango is not in fruits")

###10)List Comprehension

fruit_lengths = [len(fruit) for fruit in fruits]
print(fruit_lengths)


###11)Sort a List
fruits.sort()
print(fruits)
fruits.sort(reverse=True)
print(fruits)

###12)Nested Lists

nested_list = [fruits[:3], fruits[-3:]]
print(nested_list[1][0])


###13)Remove Duplicates

numbers = [1, 2, 2, 3, 4, 4, 4, 5]
unique_numbers = list(set(numbers))
print("The Result:",unique_numbers)


###14) Split and Join Strings

sentence = "hello, world, python, programming"
words = sentence.split(", ")
print(" ".join(words))
