# base image
FROM pelias/libpostal_baseimage

# dependencies
RUN apt-get update && \
    apt-get install -y python sqlite3 gdal-bin lftp unzip pigz time gawk && \
    rm -rf /var/lib/apt/lists/*

# --- pbf2json ---

# change working dir
ENV WORKDIR /code/pelias/interpolation
WORKDIR ${WORKDIR}

# copy package.json first to prevent npm install being rerun when only code changes
COPY ./package.json ${WORK}

# note: 'unsafe-perm' is only required when running a fork of 'better-sqlite3' as root, when
# the 'unsafe mode' PR is merged we can revert to using the prebuilt releases of the module.
RUN npm install --unsafe-perm

# add the rest of the code
ADD . ${WORKDIR}

# run tests
RUN npm test

# location where the db files will be created
ENV BUILDDIR '/data/interpolation'

# location of the openstreetmap data
ENV OSMPATH '/data/openstreetmap'

# location of the polylines data
ENV POLYLINEPATH '/data/polylines'

# location of the openaddresses data
ENV OAPATH '/data/openaddresses'

# location of TIGER data
ENV TIGERPATH '/data/tiger/'

ENV WORKINGDIR '/data'

# run as a pelias user
USER pelias

# entrypoint
CMD [ "./interpolate", "server", "/data/interpolation/address.db", "/data/interpolation/street.db" ]
