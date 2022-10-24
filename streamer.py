# do not import anything else from loss_socket besides LossyUDP
from concurrent.futures import ThreadPoolExecutor
from operator import is_
import time
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
HEADER_LENGTH = 10


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.receive_buffer = {}
        self.closed = False
        self.seq_ack_status = {}
        self.fin_received = False

        # start the listener thread
        executer = ThreadPoolExecutor(max_workers=1)
        executer.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        global SEND_SEQNUM

        max_packet_size = 1472 - HEADER_LENGTH
        while len(data_bytes) > max_packet_size:
            content = data_bytes[:max_packet_size]
            data_bytes = data_bytes[max_packet_size:]

            # struct docs: https://docs.python.org/3/library/struct.html
            # use struct to pack in sequence number, content length, and content
            curr_seqnum = SEND_SEQNUM
            data = struct.pack("II??%ds" % max_packet_size, SEND_SEQNUM, max_packet_size, False, False, content)
            # increment sequence number by the appropriate amount
            SEND_SEQNUM += max_packet_size + HEADER_LENGTH

            self.socket.sendto(data, (self.dst_ip, self.dst_port))
            self.seq_ack_status[curr_seqnum] = False
            # print("sent packet with seqnum: " + str(curr_seqnum))
            self.wait_for_ack(curr_seqnum, data)

        if len(data_bytes) > 0:
            curr_seqnum = SEND_SEQNUM
            data = struct.pack("II??%ds" % len(data_bytes), SEND_SEQNUM, len(data_bytes), False, False, data_bytes)
            SEND_SEQNUM += len(data_bytes) + HEADER_LENGTH
            self.socket.sendto(data, (self.dst_ip, self.dst_port))
            self.seq_ack_status[curr_seqnum] = False
            # print("sent packet with seqnum: " + str(curr_seqnum))
            self.wait_for_ack(curr_seqnum, data)
    
    # def send_data
    
    def wait_for_ack(self, seqnum, data):
        # Add a timeout to the wait for ack
        start_time = time.time()
        while not self.seq_ack_status or not self.seq_ack_status[seqnum]:
            # print("waiting for ack", curr_seqnum)
            if time.time() - start_time > 0.5:
                self.socket.sendto(data, (self.dst_ip, self.dst_port))
                start_time = time.time()
            time.sleep(0.01)
        
        del self.seq_ack_status[seqnum]

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        global RECV_SEQNUM

        # helper method to receive and unpack/parse a packet
        def recv_helper():
            data = self.receive_buffer[RECV_SEQNUM]
            self.receive_buffer.pop(RECV_SEQNUM)

            # unpack the headers in the packet (data)
            # (var,) format is used because struct.unpack returns a tuple
            (data_seqnum,) = struct.unpack("I", data[:4])
            (content_length,) = struct.unpack("I", data[4:8])
            (is_ack,) = struct.unpack("?", data[8:9])
            (is_fin,) = struct.unpack("?", data[9:10])
            (content,) = struct.unpack("%ds" % content_length, data[HEADER_LENGTH:])
            return data_seqnum, content_length, is_ack, is_fin, content
        while True:
            if RECV_SEQNUM in self.receive_buffer:
            # initial call to recv_helper to receive the next packet in the stream
                data_seqnum, content_length, is_ack, is_fin, content = recv_helper()

                # # debugging
                # print("packet seqnum: " + str(data_seqnum))
                # print("content length: " + str(content_length))

                # as long as out of order (later) packets are being received, keep storing them in
                # the buffer and taking in the next packet in the stream using recv_helper
                while data_seqnum != RECV_SEQNUM:
                    OOO_BUFFER.append((data_seqnum, content_length, content))
                    OOO_BUFFER.sort()
                    data_seqnum, content_length, is_ack, is_fin, content = recv_helper()

                # once the right packet has been received, update RECV_SEQNUM
                RECV_SEQNUM += content_length + HEADER_LENGTH
                # keep popping Out-Of-Order buffer to retrieve any consecutive packets until up-to-date
                while len(OOO_BUFFER) > 0 and OOO_BUFFER[0][0] == RECV_SEQNUM:
                    dsq, cl, c = OOO_BUFFER.pop(0)
                    content += c
                    RECV_SEQNUM += cl + HEADER_LENGTH

                return content
            else:
                continue

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.

        # wait for all packets to be acked
        while self.seq_ack_status:
            time.sleep(0.01)

        # send a final packet
        content = b'1'
        data = struct.pack("II??%ds" % len(content), SEND_SEQNUM, len(content), False, True, content)
        self.socket.sendto(data, (self.dst_ip, self.dst_port))

        # wait for the final ack; with a timeout
        start_time = time.time()
        while not self.fin_received:
            if time.time() - start_time > 0.5:
                self.socket.sendto(data, (self.dst_ip, self.dst_port))
                start_time = time.time()
            time.sleep(0.01)
        
        time.sleep(2)
    
        self.closed = True
        self.socket.stoprecv()
    
    def unpack(self, data):
        """Unpacks the headers in the packet (data)"""
        # unpack the headers in the packet (data)
        # (var,) format is used because struct.unpack returns a tuple
        (data_seqnum,) = struct.unpack("I", data[:4])
        (content_length,) = struct.unpack("I", data[4:8])
        (is_ack,) = struct.unpack("?", data[8:9])
        (is_fin,) = struct.unpack("?", data[9:10])
        (content,) = struct.unpack("%ds" % content_length, data[HEADER_LENGTH:])
        return data_seqnum, content_length, is_ack, is_fin, content

    def listener(self) -> None:
        """Listens for incoming packets, and prints them out to the console."""
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()

                if len(data) == 0:
                    continue
                data_seqnum, content_length, is_ack, is_fin, content = self.unpack(data)

                fin_ack_content = b'1'

                if is_ack and is_fin:
                    self.fin_received = True
                elif is_fin:
                    data = struct.pack("II??%ds" % len(fin_ack_content), data_seqnum, len(fin_ack_content), True, True, fin_ack_content)
                    self.socket.sendto(data, (self.dst_ip, self.dst_port))
                elif is_ack:
                    self.seq_ack_status[data_seqnum] = True
                else:
                    # save the packet in the receive buffer
                    self.receive_buffer[data_seqnum] = data
                    # send an ACK back to the sender
                    print("sending ack for seqnum: " + str(data_seqnum))
                    ack = struct.pack("II??%ds" % len(fin_ack_content), data_seqnum, len(fin_ack_content), True, False, fin_ack_content)
                    self.socket.sendto(ack, (self.dst_ip, self.dst_port))
            
            except Exception as e:
                print("listener died!")
                print(e)
        

            
