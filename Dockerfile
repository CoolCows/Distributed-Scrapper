FROM scrapkord-base 
WORKDIR /app
COPY . .
ENTRYPOINT ["python", "run.py"]
