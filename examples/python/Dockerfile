FROM husseinawala/spark-py:v3.5.0

# Add the job and the modules
ADD my_modules /opt/spark/work-dir/my_modules
ADD job.py /opt/spark/work-dir/
# Update the PYTHONPATH to include the modules
ENV PYTHONPATH "${PYTHONPATH}:/opt/spark/work-dir"
