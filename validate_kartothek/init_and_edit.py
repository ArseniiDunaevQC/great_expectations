import subprocess
import great_expectations as ge

subprocess.run(["great_expectations", "init"])

context = ge.data_context.DataContext()

expectations_suites = context.list_expectation_suite_names()  # after 'init' there is only one

subprocess.run(["great_expectations", "suite", "edit", expectations_suites[0]])
