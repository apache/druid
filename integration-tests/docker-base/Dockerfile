FROM ubuntu:16.04

# Bundle everything into one script so cleanup can reduce image size.
# Otherwise docker's layered images mean that things are not actually deleted.

COPY setup.sh /root/setup.sh
RUN chmod 0755 /root/setup.sh && /root/setup.sh
