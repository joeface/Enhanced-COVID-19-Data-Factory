FROM ubuntu:latest
MAINTAINER Mikhail Ageev


# Install cron
RUN apt-get update && apt-get install -y cron python3 build-essential python3-dev python3-pip python3-venv

# Copy files
COPY main.py /opt/
COPY requirements.txt /opt/
COPY world-map-geo.json /opt/
COPY entrypoint.sh /opt/
COPY schedule /etc/cron.d/covid



# Set up and activate virtual environment
ENV VIRTUAL_ENV "/venv"
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH "$VIRTUAL_ENV/bin:$PATH"
ENV MANUAL_DATA_SOURCE_URL "https://docs.google.com/spreadsheets/d/e/2PACX-1vTRXudbSwPQY2DJDcYQ3Rot7TxLR1I8HzepeRuhU6VRAcVCnKKDS7wNvku0VlX0yg_fv7eiXpd41YWK/pub?gid=0&single=true&output=csv"


# Install Python packages and run app
RUN python -m pip install -r /opt/requirements.txt --no-cache-dir

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Initial app execution
#CMD [ "python", "/opt/main.py" ]


# Execute Entrypoint script
RUN chmod +x /opt/entrypoint.sh
ENTRYPOINT /opt/entrypoint.sh