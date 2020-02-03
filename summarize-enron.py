from bnp_mails_spark import Application
from sys import argv

Application().main(argv[1])

print("Press Enter to stop SparkUI...")
input()
