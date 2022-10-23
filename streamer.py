# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
# other libraries
import struct

# GLOBAL VARIABLES
# set both initial sequence numbers to 0
# RECV_SEQNUM, so far, keeps track of the expected seqnum of next received packet.
SEND_SEQNUM = 0
RECV_SEQNUM = 0
# use normal list for storing Out-Of-Order (OOO) packets. list will be sorted later with sort().
OOO_BUFFER = []
# 4 bytes needed for sequence number, 4 bytes needed for content length
# change value as needed later for additional headers
HEADER_LENGTH = 8


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        global SEND_SEQNUM

        max_packet_size = 1472 - HEADER_LENGTH
        while len(data_bytes) > max_packet_size:
            content = data_bytes[:max_packet_size]
            data_bytes = data_bytes[max_packet_size:]

            # struct docs: https://docs.python.org/3/library/struct.html
            # use struct to pack in sequence number, content length, and content
            data = struct.pack("II%ds" % max_packet_size, SEND_SEQNUM, max_packet_size, content)
            # increment sequence number by the appropriate amount
            SEND_SEQNUM += max_packet_size + HEADER_LENGTH

            self.socket.sendto(data, (self.dst_ip, self.dst_port))

        if len(data_bytes) > 0:
            data = struct.pack("II%ds" % len(data_bytes), SEND_SEQNUM, len(data_bytes), data_bytes)
            SEND_SEQNUM += len(data_bytes) + HEADER_LENGTH
            self.socket.sendto(data, (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        global RECV_SEQNUM

        # helper method to receive and unpack/parse a packet
        def recv_helper():
            data, addr = self.socket.recvfrom()

            # unpack the headers in the packet (data)
            # (var,) format is used because struct.unpack returns a tuple
            (data_seqnum,) = struct.unpack("I", data[:4])
            (content_length,) = struct.unpack("I", data[4:8])
            (content,) = struct.unpack("%ds" % content_length, data[HEADER_LENGTH:])
            return data_seqnum, content_length, content

        # initial call to recv_helper to receive the next packet in the stream
        data_seqnum, content_length, content = recv_helper()

        # # debugging
        # print("packet seqnum: " + str(data_seqnum))
        # print("content length: " + str(content_length))

        # as long as out of order (later) packets are being received, keep storing them in
        # the buffer and taking in the next packet in the stream using recv_helper
        while data_seqnum != RECV_SEQNUM:
            OOO_BUFFER.append((data_seqnum, content_length, content))
            OOO_BUFFER.sort()
            data_seqnum, content_length, content = recv_helper()

        # once the right packet has been received, update RECV_SEQNUM
        RECV_SEQNUM += content_length + HEADER_LENGTH
        # keep popping Out-Of-Order buffer to retrieve any consecutive packets until up-to-date
        while len(OOO_BUFFER) > 0 and OOO_BUFFER[0][0] == RECV_SEQNUM:
            dsq, cl, c = OOO_BUFFER.pop(0)
            content += c
            RECV_SEQNUM += cl + HEADER_LENGTH

        return content

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
