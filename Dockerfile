FROM python:3.10-slim
# # opencv is installed then uninstalled apparently because it gives us some dependencies that we need
# # then we reinstall opencv using pip. Idk ask the original authors
# RUN apt-get update -qyy && \
#     apt-get install -y  python3-opencv && apt-get remove -y python3-opencv \
#     && pip install --upgrade pip \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /src/redis_streamer
ADD requirements.txt .
RUN pip install -r requirements.txt && rm -rf ~/.cache/pip /var/cache/apt/
# RUN pip install -r https://raw.githubusercontent.com/ultralytics/yolov5/master/requirements.txt  # install yolo dependencies

ENV PYTHONPATH "${PYTHONPATH}:/src"

ADD ./redis_streamer /src/redis_streamer
RUN mkdir -p ./data

# ENTRYPOINT [ "uvicorn" ]
# CMD ["redis_streamer.main:app", "--host", "0.0.0.0", "--reload"]
ENTRYPOINT [ "python" ]
CMD [ "-m", "gunicorn", "redis_streamer.main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000" ]