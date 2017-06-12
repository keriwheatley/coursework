import datetime
import sys
from subprocess import Popen, PIPE
from random import choices

def main():
  try:

    # Check for 2 inputs (ex. python mumbler.py hello 3)
    if len(sys.argv) == 3:

      # Set inputs as variables
      total_words = int(sys.argv[2])
      current_word = sys.argv[1]

      print("Start at " + str(datetime.datetime.now()))
      
      # Variable to count number of iterations
      next_word_increment = 0

      # Loop through process until next_word_increment = total_words
      # or no more two-grams are found
      while next_word_increment < total_words:

        # Use grep to find all two-grams that start with current_word
        # and use subprocess.Popen to pipe outputs 
        # command1 = ['grep', '^'+current_word+' ', '/gpfs/gpfsfpo/preprocessed_files/\*']
        command1 = 'grep ^"'+current_word+' " /gpfs/gpfsfpo/preprocessed_files/*'
        process1 = Popen(command1,stdout=PIPE,shell=True)

        # Store secord words in dictionary and sum second word counts
        word_dict={}
        while True:
          line = process1.stdout.readline()
          if line != b'':
            second_word = line.split()[1]
            count = int(line.split()[2])
            if second_word in word_dict: word_dict[second_word] += count
            else: word_dict[second_word] = count
          else:
            break
        
        # If dictionary is empty, end the function
        if not word_dict:
          print ("No two-grams found with current word.")
          print("End at " + str(datetime.datetime.now()))
          return

        # Use random.choices weighted population functionality to pick next word
        current_word = choices(list(word_dict), weights=word_dict.values(),k=1)
        current_word = current_word[0].decode("utf-8")
        print(current_word)
        
        next_word_increment += 1
         
    else:
      print("This command requires 2 inputs. See example: python mumbler.py hello 3")
      return

  except Exception as inst:
    print(inst.args)
    print(inst)

if __name__ == '__main__':
  main() 
