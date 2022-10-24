# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
# other libraries
import struct
from concurrent.futures import ThreadPoolExecutor
import time
import hashlib
from threading import Timer
# GLOBAL VARIABLES
# set both initial sequence numbers to 0
# RECV_SEQNUM, so far, keeps track of the expected seqnum of next received packet.
SEND_SEQNUM = 0
RECV_SEQNUM = 0
# use normal list for storing Out-Of-Order (OOO) packets. list will be sorted later with sort().
OOO_BUFFER = []
# 4 bytes needed for sequence number, 4 bytes needed for content length
# change value as needed later for additional headers
HEADER_LENGTH = 26


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
        self.ack = False
        self.ack_queue = []

        # start the listener thread
        excecuter = ThreadPoolExecutor(max_workers=1)
        excecuter.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""

        global SEND_SEQNUM
        sequence_number = SEND_SEQNUM
        update_seq = self.send_data(sequence_number,data_bytes)

        # wait for ACK; Replace with selective repeat
        # Add timeout
        # Timer(0.25, self.selective_repeat, [sequence_number, data_bytes]).start()
        timer = time.time()
        update_seq = self.time_out_loop(timer,update_seq, sequence_number, data_bytes)
        SEND_SEQNUM = update_seq
    
    def selective_repeat(self, sequence_number, data_bytes):
        if self.closed:
            return
        if sequence_number in self.ack_queue:
            self.send_data(sequence_number, data_bytes)


    
    def time_out_loop(self, timer,update_seq, sequence_number, data_bytes):
        while not self.ack:
            if time.time() - timer > 0.25:
                # print("timeout", self.ack_queue)
                if len(self.ack_queue) > 0: self.ack_queue.remove(sequence_number)
                update_seq = self.send_data(sequence_number, data_bytes)
                timer = time.time()
            time.sleep(0.01)
        self.ack = False

        return update_seq
    
    def send_data(self, sequence_num, data_bytes):

        max_packet_size = 1472 - HEADER_LENGTH
        while len(data_bytes) > max_packet_size:
            content = data_bytes[:max_packet_size]
            data_bytes = data_bytes[max_packet_size:]

            # struct docs: https://docs.python.org/3/library/struct.html
            # use struct to pack in sequence number, content length, and content
            data = self.create_packet(max_packet_size, sequence_num, False, False, content)
            self.ack_queue.append(sequence_num)
            # increment sequence number by the appropriate amount
            sequence_num += max_packet_size + HEADER_LENGTH

            self.socket.sendto(data, (self.dst_ip, self.dst_port))

        if len(data_bytes) > 0:
            data = self.create_packet(len(data_bytes), sequence_num, False, False, data_bytes)
            self.ack_queue.append(sequence_num)
            sequence_num += len(data_bytes) + HEADER_LENGTH
            self.socket.sendto(data, (self.dst_ip, self.dst_port))
        
        # print("pending acks", self.ack_queue)
        return sequence_num
    
    def create_packet(self, length, seqnum,is_ack, is_fin, content):
        """
        :param length: length of the content
        :param seqnum: sequence number of the packet
        :param content: content of the packet
        :param is_ack: boolean indicating whether the packet is an ACK
        :return: a packet with the appropriate headers

        Format of the packet:
            I: Unsigned integer
            ?: Boolean
            %ds: String of length content_length
        """
        content_hash = self.hash_md5(content)
        return struct.pack("II??", seqnum, length, is_ack, is_fin) + content_hash + struct.pack("%ds" % length, content)
    
    def hash_md5(self, data):
        # create hash of the content
        m = hashlib.md5()         
        m.update(data)         
        content_hash = m.digest()
        return content_hash
    
    def unpack_packet(self, data):
        """
        :param data: the packet to unpack
        :return: the sequence number, content length, and content of the packet
        """
        (data_seqnum,) = struct.unpack("I", data[:4])
        (content_length,) = struct.unpack("I", data[4:8])
        (is_ack,) = struct.unpack("?", data[8:9])
        (is_fin,) = struct.unpack("?", data[9:10])
        (content_hash, ) = struct.unpack("%ds" % 16, data[10:26])
        (content,) = struct.unpack("%ds" % content_length, data[HEADER_LENGTH:])
        return data_seqnum, content_length, is_ack, is_fin, content_hash, content

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        global RECV_SEQNUM
        while True:
            if RECV_SEQNUM in self.receive_buffer:
                # initial call to recv_helper to receive the next packet in the stream

                data = self.receive_buffer[RECV_SEQNUM]
                self.receive_buffer.pop(RECV_SEQNUM)
                # unpack the headers in the packet (data)
                data_seqnum, content_length, is_ack, is_fin, content_hash, content = self.unpack_packet(data)

                # once the right packet has been received, update RECV_SEQNUM
                RECV_SEQNUM += content_length + HEADER_LENGTH
                # keep popping Out-Of-Order buffer to retrieve any consecutive packets until up-to-date
                while len(OOO_BUFFER) > 0 and OOO_BUFFER[0][0] == RECV_SEQNUM:
                    dsq, cl, a, f, ch, c = OOO_BUFFER.pop(0)
                    content += c
                    RECV_SEQNUM += cl + HEADER_LENGTH
                return content
            else:
                continue
            
    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.

        global SEND_SEQNUM
        # check if all packets have been ACKed
        # if not, wait for ACKs
        while len(self.ack_queue) > 0:
            time.sleep(0.01)
        
        print("all ack received")
        # send FIN packet
        data = self.create_packet(0, SEND_SEQNUM, False, True, b'')
        self.socket.sendto(data, (self.dst_ip, self.dst_port))
        self.ack_queue.append(SEND_SEQNUM)
        print("fin sent")
        # wait for ACK, set timer
        start_time = time.time()
        while self.ack == False:
            if time.time() - start_time > 0.25:
                self.socket.sendto(data, (self.dst_ip, self.dst_port))
                start_time = time.time()
            time.sleep(0.01)
        print("fin ack received")
        time.sleep(2)

        self.closed = True
        self.socket.stoprecv()

    def listener(self) -> None:
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()

                if len(data) == 0:
                    continue

                data_seqnum, content_length, is_ack, is_fin, content_hash, content = self.unpack_packet(data)

                # hash test
                if content_hash != self.hash_md5(content):
                    continue

                if is_ack:
                    if data_seqnum in self.ack_queue:
                        self.ack = True
                        # print("ack received for", data_seqnum)
                        self.ack_queue.remove(data_seqnum)
                elif is_fin:
                    # send ACK for FIN
                    fin_data = self.create_packet(0, data_seqnum, True, False, b'')
                    self.socket.sendto(fin_data, (self.dst_ip, self.dst_port))
                else:
                    self.receive_buffer[data_seqnum] = data
                    if data_seqnum != RECV_SEQNUM:
                        OOO_BUFFER.append((data_seqnum, content_length, is_ack, is_fin, content_hash, content))
                        OOO_BUFFER.sort()        
                    self.socket.sendto(self.create_packet(0, data_seqnum, True, False, b''), (self.dst_ip, self.dst_port))

            except Exception as e:
                print("listener died!")
                print(e)
        
        self.socket.stoprecv()
        

