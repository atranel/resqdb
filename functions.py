import os
import time 

def save_file(name, data=None, index=False):
    """ If the file already exists, first try to rename it, it renaming is succes, rename it back and resaved the file else raise error and print warning to user, wait 2 seconds and try it again. 

    :params name: name of results file
    :type name: string
    :params data: dataframe to be saved
    :type data: dataframe
    :params index: iclude index in the file
    :type index: boolean
    """
    path = os.path.join(os.getcwd(), name)
    if os.path.exists(path):
        while True:
            time.sleep(10)
            closed = False
            try: 
                os.rename(path, f'{path}_')
                closed = True
                os.rename(f'{path}_',path)
            except IOError:
                print("Couldn't save file! Please, close the file {0}!".format(name))
            
            if closed:
                if data is not None:  
                    data.to_csv(path, sep=",", encoding='utf-8', index=index)
                break
    else:
        if data is not None:
            data.to_csv(path, sep=",", encoding='utf-8', index=index)


def repeat_answer():
    """ Return True if the user would like to continue but with diferrent setting otherwise return False."""
    while True:
        repeat = input("Would you like to continue with different setting? (y/n)\n").lower()
        if repeat != "y" and repeat != "n":
            print("Wrong option!")
            continue
        elif repeat == "y":
            repeat_calculation = True
            break
        else:
            repeat_calculation = False
            break

    return repeat_calculation