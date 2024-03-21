import sys

def write_to_file(param):
    with open('a.txt', 'w') as file:
        file.write(param)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        param_value = sys.argv[1]
        write_to_file(param_value)
    else:
        print("No parameter provided.")