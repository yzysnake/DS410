def q1(mystring):
    """ split the string by tabs to get an array and return the array """
    mylist = mystring.split("\t")
    return mylist


def q2(mystring):
    """ split the string by tabs to get an array and return the second element of the array """
    mylist = mystring.split("\t")
    return mylist[1]


def q3(myarray):
    """ myarray is an list of pairs. this function should return the sum of the first
    items in the pair and the sum of the second items """
    first_sum = 0
    second_sum = 0
    for i in myarray:
        first_sum += i[0]
        second_sum += i[1]
    return first_sum, second_sum


def q4(mystringarray):
    """ return the position of the first occurrence of the string 'hi' or -1 if it is not found.
    you cannot change how the array is iterated and you cannot use any list operations on mystringarray"""
    index = 0
    for mystring in mystringarray:
        if mystring == 'hi':
            return index
        else:
            index += 1
    return -1


def q5(myarray):
    """ return a dictionary containing the counts of items in the input array """
    dictionary = {}
    for i in myarray:
        if i in dictionary:
            dictionary[i] += 1
        else:
            dictionary[i] = 1
    return dictionary