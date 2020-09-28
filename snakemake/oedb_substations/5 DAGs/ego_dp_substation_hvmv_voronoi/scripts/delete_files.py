import os

def delete_files():

    os.remove("/home/sharonov/snakemake/first.txt")
    os.remove("/home/sharonov/snakemake/second.txt")
    os.remove("/home/sharonov/snakemake/third.txt")
    os.remove("/home/sharonov/snakemake/fourth.txt")

delete_files()
