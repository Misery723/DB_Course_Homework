# -----------------------------------------------------------------------
# storage_db.py
# Author: Jingyu Han  hjymail@163.com
# -----------------------------------------------------------------------
# the module is to store tables in files
# Each table is stored in a separate file with the suffix ".dat".
# For example, the table named moviestar is stored in file moviestar.dat 
# -----------------------------------------------------------------------

# struct of file is as follows, each block is 4096
# ---------------------------------------------------
# block_0|block_1|...|block_n
# ----------------------------------------------------------------
from common_db import BLOCK_SIZE

# structure of block_0, which stores the meta information and field information
# ---------------------------------------------------------------------------------
# block_id                                # 0
# number_of_dat_blocks                    # at first it is 0 because there is no data in the table
# number_of_fields or number_of_records   # the total number of fields for the table
# -----------------------------------------------------------------------------------------


# the data type is as follows
# ----------------------------------------------------------
# 0->str,1->varstr,2->int,3->bool
# ---------------------------------------------------------------


# structure of data block, whose block id begins with 1
# ----------------------------------------
# block_id       
# number of records
# record_0_offset         # it is a pointer to the data of record
# record_1_offset
# ...
# record_n_offset
# ....
# free space
# ...
# record_n
# ...
# record_1
# record_0
# -------------------------------------------

# structre of one record
# -----------------------------
# pointer                     #offset of table schema in block id 0
# length of record            # including record head and record content
# time stamp of last update  # for example,1999-08-22
# field_0_value
# field_1_value
# ...
# field_n_value
# -------------------------


import struct
import os
import ctypes


# --------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
# --------------------------------------------

class Storage(object):

    # ------------------------------
    # constructor of the class
    # input:
    #       tablename
    # -------------------------------------
    def __init__(self, tablename):
        # print "__init__ of ",Storage.__name__,"begins to execute"
        tablename.strip()

        self.record_list = []
        self.record_Position = []

        if not os.path.exists(tablename + '.dat'.encode('utf-8')):  # the file corresponding to the table does not exist
            print('table file '.encode('utf-8') + tablename + '.dat does not exists'.encode('utf-8'))
            self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'wb+')
            self.f_handle.close()
            self.open = False
            print(tablename + '.dat has been created'.encode('utf-8'))

        self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'rb+')
        print('table file '.encode('utf-8') + tablename + '.dat has been opened'.encode('utf-8'))
        self.open = True

        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.f_handle.seek(0)
        self.dir_buf = self.f_handle.read(BLOCK_SIZE)

        self.dir_buf.strip()
        my_len = len(self.dir_buf)
        self.field_name_list = []
        beginIndex = 0

        if my_len == 0:  # there is no data in the block 0, we should write meta data into the block 0
            if isinstance(tablename, bytes):
                self.num_of_fields = input(
                    "please input the number of feilds in table " + tablename.decode('utf-8') + ":")
            else:
                self.num_of_fields = input(
                    "please input the number of feilds in table " + tablename + ":")
            if int(self.num_of_fields) > 0:

                self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                self.block_id = 0
                self.data_block_num = 0
                struct.pack_into('!iii', self.dir_buf, beginIndex, 0, 0,
                                 int(self.num_of_fields))  # block_id,number_of_data_blocks,number_of_fields

                beginIndex = beginIndex + struct.calcsize('!iii')

                # the following is to write the field name,field type and field length into the buffer in turn
                for i in range(int(self.num_of_fields)):
                    field_name = input("please input the name of field " + str(i) + " :")

                    if len(field_name) < 10:
                        field_name = ' ' * (10 - len(field_name.strip())) + field_name

                    while True:
                        field_type = input(
                            "please input the type of field(0-> str; 1-> varstr; 2-> int; 3-> boolean) " + str(
                                i) + " :")
                        if int(field_type) in [0, 1, 2, 3]:
                            break

                    # to need further modification here
                    field_length = input("please input the length of field " + str(i) + " :")
                    temp_tuple = (field_name, int(field_type), int(field_length))
                    self.field_name_list.append(temp_tuple)
                    if isinstance(field_name, str):
                        field_name = field_name.encode('utf-8')

                    struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, int(field_type),
                                     int(field_length))
                    beginIndex = beginIndex + struct.calcsize('!10sii')

                self.f_handle.seek(0)
                self.f_handle.write(self.dir_buf)
                self.f_handle.flush()

        else:  # there is something in the file

            self.block_id, self.data_block_num, self.num_of_fields = struct.unpack_from('!iii', self.dir_buf, 0)

            print('number of fields is ', self.num_of_fields)
            print('data_block_num', self.data_block_num)
            beginIndex = struct.calcsize('!iii')

            # the followins is to read field name, field type and field length into main memory structures
            for i in range(self.num_of_fields):
                field_name, field_type, field_length = struct.unpack_from('!10sii', self.dir_buf,
                                                                          beginIndex + i * struct.calcsize(
                                                                              '!10sii'))  # i means no memory alignment

                temp_tuple = (field_name, field_type, field_length)
                self.field_name_list.append(temp_tuple)
                print("the " + str(i) + "th field information (field name,field type,field length) is ", temp_tuple)
        # print self.field_name_list
        record_head_len = struct.calcsize('!ii10s')
        record_content_len = sum(map(lambda x: x[2], self.field_name_list))
        # print record_content_len

        Flag = 1
        while Flag <= self.data_block_num:
            self.f_handle.seek(BLOCK_SIZE * Flag)
            self.active_data_buf = self.f_handle.read(BLOCK_SIZE)
            self.block_id, self.Number_of_Records = struct.unpack_from('!ii', self.active_data_buf, 0)
            print('Block_ID=%s,   Contains %s data' % (self.block_id, self.Number_of_Records))
            # There exists record
            if self.Number_of_Records > 0:
                for i in range(self.Number_of_Records):
                    self.record_Position.append((Flag, i))
                    offset = \
                        struct.unpack_from('!i', self.active_data_buf,
                                           struct.calcsize('!ii') + i * struct.calcsize('!i'))[
                            0]
                    record = struct.unpack_from('!' + str(record_content_len) + 's', self.active_data_buf,
                                                offset + record_head_len)[0]
                    tmp = 0
                    tmpList = []
                    for field in self.field_name_list:
                        t = record[tmp:tmp + field[2]].strip()
                        tmp = tmp + field[2]
                        if field[1] == 2:
                            t = int(t)
                        if field[1] == 3:
                            t = bool(t)
                        tmpList.append(t)
                    self.record_list.append(tuple(tmpList))
            Flag += 1

    # ------------------------------
    # return the record list of the table
    # input:
    #       
    # -------------------------------------
    def getRecord(self):
        return self.record_list

    # --------------------------------
    # to insert a record into table
    # param insert_record: list
    # return: True or False
    # -------------------------------
    def insert_record(self, insert_record):

        # example: ['xuyidan','23','123456']

        # step 1 : to check the insert_record is True or False

        tmpRecord = []
        for idx in range(len(self.field_name_list)):
            insert_record[idx] = insert_record[idx].strip()
            if self.field_name_list[idx][1] == 0 or self.field_name_list[idx][1] == 1:
                if len(insert_record[idx]) > self.field_name_list[idx][2]:
                    return False
                tmpRecord.append(insert_record[idx])
            if self.field_name_list[idx][1] == 2:
                try:
                    tmpRecord.append(int(insert_record[idx]))
                except:
                    return False
            if self.field_name_list[idx][1] == 3:
                try:
                    tmpRecord.append(bool(insert_record[idx]))
                except:
                    return False
            insert_record[idx] = ' ' * (self.field_name_list[idx][2] - len(insert_record[idx])) + insert_record[idx]

        # step2: Add tmpRecord to record_list ; change insert_record into inputstr
        inputstr = ''.join(insert_record)

        self.record_list.append(tuple(tmpRecord))

        # Step3: To calculate MaxNum in each Data Blocks
        record_content_len = len(inputstr)
        record_head_len = struct.calcsize('!ii10s')
        record_len = record_head_len + record_content_len
        MAX_RECORD_NUM = (BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) / (
                record_len + struct.calcsize('!i'))

        # Step4: To calculate new record Position
        if not len(self.record_Position):
            self.data_block_num += 1
            self.record_Position.append((1, 0))
        else:
            last_Position = self.record_Position[-1]
            if last_Position[1] == MAX_RECORD_NUM - 1:
                self.record_Position.append((last_Position[0] + 1, 0))
                self.data_block_num += 1
            else:
                self.record_Position.append((last_Position[0], last_Position[1] + 1))

        last_Position = self.record_Position[-1]

        # Step5: Write new record into file xxx.dat
        # update data_block_num
        self.f_handle.seek(0)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data block head
        self.f_handle.seek(BLOCK_SIZE * last_Position[0])
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, last_Position[0], last_Position[1] + 1)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data offset
        offset = struct.calcsize('!ii') + last_Position[1] * struct.calcsize('!i')
        beginIndex = BLOCK_SIZE - (last_Position[1] + 1) * record_len
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + offset)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!i'))
        struct.pack_into('!i', self.buf, 0, beginIndex)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data
        record_schema_address = struct.calcsize('!iii')
        update_time = '2016-11-16'  # update time
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + beginIndex)
        self.buf = ctypes.create_string_buffer(record_len)
        struct.pack_into('!ii10s', self.buf, 0, record_schema_address, record_content_len, update_time.encode('utf-8'))
        struct.pack_into('!' + str(record_content_len) + 's', self.buf, record_head_len, inputstr.encode('utf-8'))
        self.f_handle.write(self.buf.raw)
        self.f_handle.flush()

        return True

    # ------------------------------
    # show the data structure and its data
    # input:
    #       t
    # -------------------------------------

    def show_table_data(self):
        print('|    '.join(map(lambda x: x[0].decode('utf-8').strip(), self.field_name_list)))  # show the structure

        # the following is to show the data of the table
        for record in self.record_list:
            print(record)

    # --------------------------------
    # to delete  the data file
    # input
    #       table name
    # output
    #       True or False
    # -----------------------------------
    def delete_table_data(self, tableName):

        # step 1: identify whether the file is still open
        if self.open == True:
            self.f_handle.close()
            self.open = False

        # step 2: remove the file from os   
        tableName.strip()
        if os.path.exists(tableName + '.dat'.encode('utf-8')):
            os.remove(tableName + '.dat'.encode('utf-8'))

        return True

    # ------------------------------
    # get the list of field information, each element of which is (field name, field type, field length)
    # input:
    #       
    # -------------------------------------

    def getFieldList(self):
        return self.field_name_list

    # ----------------------------------------
    # destructor
    # ------------------------------------------------
    def __del__(self):  # write the metahead information in head object to file

        if self.open == True:
            self.f_handle.seek(0)
            self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
            struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
            self.f_handle.write(self.buf)
            self.f_handle.flush()
            self.f_handle.close()

    
    def delete_record(self, condition_field, keyword):
        # 1. 查找条件字段的索引
        field_index = -1
        condition_field = condition_field.strip().lower()
        
        # 遍历所有字段，找到匹配的字段索引
        for idx, field in enumerate(self.field_name_list):
            # 处理字段名（字节串或字符串）
            field_name = field[0].strip()
            if isinstance(field_name, bytes):
                field_name = field_name.decode('utf-8').strip().lower()
            else:
                field_name = str(field_name).strip().lower()
            
            if field_name == condition_field:
                field_index = idx
                break
        
        if field_index == -1:
            print(f"Field '{condition_field}' not found in table")
            return False
        
        # 2. 查找要删除的记录
        records_to_delete = []
        keyword = keyword.strip().lower()
        
        # 遍历所有记录，找到匹配的记录
        for i, record in enumerate(self.record_list):
            # 获取字段值并规范化
            field_value = record[field_index]
            if isinstance(field_value, bytes):
                field_value = field_value.decode('utf-8').strip().lower()
            elif isinstance(field_value, str):
                field_value = field_value.strip().lower()
            else:
                field_value = str(field_value).strip().lower()
            
            if field_value == keyword:
                records_to_delete.append(i)
        
        if not records_to_delete:
            print(f"No records found with {condition_field}={keyword}")
            return False
        
        # 3. 直接从文件删除记录（不依赖内存备份）
        # 重新打开文件进行读写
        self.f_handle.close()
        self.f_handle = open(self.f_handle.name, 'rb+')
        
        # 遍历所有数据块
        for block_id in range(1, self.data_block_num + 1):
            # 读取数据块
            self.f_handle.seek(BLOCK_SIZE * block_id)
            block_data = self.f_handle.read(BLOCK_SIZE)
            if not block_data:
                continue
            
            # 解析块头信息
            block_header = struct.unpack_from('!ii', block_data, 0)
            block_records = block_header[1]
            
            # 读取记录偏移量
            record_offsets = []
            offset_start = struct.calcsize('!ii')
            for i in range(block_records):
                offset = struct.unpack_from('!i', block_data, offset_start + i * 4)[0]
                record_offsets.append(offset)
            
            # 计算记录大小
            record_head_len = struct.calcsize('!ii10s')
            record_content_len = sum(field[2] for field in self.field_name_list)
            record_size = record_head_len + record_content_len
            
            # 创建新块缓冲区
            new_block_data = bytearray(BLOCK_SIZE)
            struct.pack_into('!ii', new_block_data, 0, block_id, 0)  # 初始记录数为0
            new_offsets_start = struct.calcsize('!ii')
            new_record_start = BLOCK_SIZE
            
            # 遍历当前块中的所有记录
            for i in range(block_records):
                # 检查是否为要删除的记录
                record_pos = (block_id - 1, i)
                if i + (block_id - 1) * block_records in records_to_delete:
                    continue  # 跳过要删除的记录
                
                # 读取记录数据
                record_data = block_data[record_offsets[i]:record_offsets[i] + record_size]
                
                # 添加到新块
                new_record_start -= record_size
                new_block_data[new_record_start:new_record_start + record_size] = record_data
                
                # 记录偏移量
                struct.pack_into('!i', new_block_data, new_offsets_start, new_record_start)
                new_offsets_start += 4
                new_records = struct.unpack_from('!i', new_block_data, 4)[0] + 1
                struct.pack_into('!i', new_block_data, 4, new_records)  # 更新记录数
            
            # 写入修改后的数据块
            self.f_handle.seek(BLOCK_SIZE * block_id)
            self.f_handle.write(new_block_data)
        
        # 4. 更新内存状态
        for idx in sorted(records_to_delete, reverse=True):
            del self.record_list[idx]
        
        # 5. 更新块头信息
        self.f_handle.seek(0)
        header_data = self.f_handle.read(struct.calcsize('!iii'))
        block0_id, data_block_num, num_fields = struct.unpack('!iii', header_data)
        
        # 更新数据块数量（可能减少）
        new_data_block_num = 0
        for block_id in range(1, data_block_num + 1):
            self.f_handle.seek(BLOCK_SIZE * block_id)
            block_header = self.f_handle.read(struct.calcsize('!ii'))
            if not block_header:
                break
            block_records = struct.unpack('!i', block_header[4:8])[0]
            if block_records > 0:
                new_data_block_num += 1
        
        # 更新文件头
        self.f_handle.seek(0)
        self.f_handle.write(struct.pack('!iii', 0, new_data_block_num, num_fields))
        self.f_handle.flush()
        
        # 更新内存中的块计数
        self.data_block_num = new_data_block_num
        
        print(f"Deleted {len(records_to_delete)} records successfully")
        return True

    # 辅助方法：重写数据文件
    def _rewrite_data_file(self):
        # 1. Close the current file handle
        self.f_handle.close()
        
        # 2. Get the original file name
        original_file_name = self.f_handle.name
        
        # 3. Delete the old file
        os.remove(original_file_name)
        
        # 4. Reopen the file in write mode
        self.f_handle = open(original_file_name, 'wb+')
        self.open = True
        
        # 5. Reinitialize the table structure
        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.block_id, self.data_block_num, self.num_of_fields = 0, 0, len(self.field_name_list)
        
        # Write meta data to block 0
        struct.pack_into('!iii', self.dir_buf, 0, 0, 0, self.num_of_fields)
        beginIndex = struct.calcsize('!iii')
        
        for i, field in enumerate(self.field_name_list):
            field_name, field_type, field_length = field
            # Ensure field name is bytes
            if isinstance(field_name, str):
                field_name = field_name.encode('utf-8')
            struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, field_type, field_length)
            beginIndex += struct.calcsize('!10sii')
        
        self.f_handle.seek(0)
        self.f_handle.write(self.dir_buf)
        self.f_handle.flush()
        
        # 6. Reset record tracking
        self.record_list = []
        self.record_Position = []
        
        # 7. Reinsert all current records
        for record in self.record_list_backup:
            # Convert record to string list format
            record_str = []
            for i, value in enumerate(record):
                # Handle different data types
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                elif isinstance(value, int):
                    value = str(value)
                elif isinstance(value, bool):
                    value = str(value).lower()
                record_str.append(value)
            
            # Insert the record
            if not self.insert_record(record_str):
                print(f"Failed to reinsert record: {record_str}")
        
        # 8. Update state
        print(f"Rewrote data file with {len(self.record_list)} records")



    def update_record(self, condition_field, old_value, update_field, new_value):
        # 1. 查找条件字段的索引
        cond_field_index = -1
        update_field_index = -1
        
        # 遍历字段列表，查找匹配字段
        for idx, field in enumerate(self.field_name_list):
            field_name = field[0].strip()
            if isinstance(field_name, bytes):
                field_name = field_name.decode('utf-8').strip()
            
            if field_name == condition_field.strip():
                cond_field_index = idx
            if field_name == update_field.strip():
                update_field_index = idx
        
        if cond_field_index == -1 or update_field_index == -1:
            print(f"Field not found. Condition: {condition_field}, Update: {update_field}")
            return False
        
        # 2. 查找匹配的记录位置
        record_positions = []
        block_data = {}
        
        # 遍历所有数据块
        for block_id in range(1, self.data_block_num + 1):
            # 读取数据块
            self.f_handle.seek(BLOCK_SIZE * block_id)
            block_content = self.f_handle.read(BLOCK_SIZE)
            
            # 解析块头信息
            block_id_in_file, num_records = struct.unpack('!ii', block_content[:8])
            record_offsets = []
            
            # 读取所有记录的偏移量
            offset_start = 8
            for i in range(num_records):
                offset = struct.unpack_from('!i', block_content, offset_start)[0]
                record_offsets.append(offset)
                offset_start += 4
            
            # 计算记录大小
            record_head_size = struct.calcsize('!ii10s')
            record_content_size = sum(field[2] for field in self.field_name_list)
            record_size = record_head_size + record_content_size
            
            # 遍历记录
            for record_idx, offset in enumerate(record_offsets):
                # 读取记录头
                record_header = block_content[offset:offset + record_head_size]
                schema_addr, record_len, timestamp = struct.unpack('!ii10s', record_header)
                
                # 读取记录内容
                record_content = block_content[offset + record_head_size:offset + record_size]
                
                # 提取条件字段的值
                field_start = sum(field[2] for field in self.field_name_list[:cond_field_index])
                field_end = field_start + self.field_name_list[cond_field_index][2]
                cond_value = record_content[field_start:field_end].strip()
                
                # 检查是否匹配
                if cond_value == old_value.encode('utf-8') or cond_value.decode('utf-8') == old_value:
                    record_positions.append((block_id, record_idx, offset))
        
        if not record_positions:
            print(f"No matching records found for {condition_field}={old_value}")
            return False
        
        # 3. 直接修改文件中的记录
        for block_id, record_idx, offset in record_positions:
            # 移动到记录位置
            self.f_handle.seek(BLOCK_SIZE * block_id + offset)
            
            # 读取记录头
            record_header = self.f_handle.read(record_head_size)
            schema_addr, record_len, timestamp = struct.unpack('!ii10s', record_header)
            
            # 读取记录内容
            record_content = self.f_handle.read(record_content_size)
            
            # 转换为可修改的字节数组
            record_bytes = bytearray(record_content)
            
            # 计算更新字段的位置
            field_start = sum(field[2] for field in self.field_name_list[:update_field_index])
            field_length = self.field_name_list[update_field_index][2]
            
            # 确保新值长度正确
            if len(new_value) > field_length:
                new_value = new_value[:field_length]
            elif len(new_value) < field_length:
                new_value = new_value.ljust(field_length)
            
            # 更新字段值
            new_value_bytes = new_value.encode('utf-8')
            record_bytes[field_start:field_start + field_length] = new_value_bytes
            
            # 写回修改后的记录
            self.f_handle.seek(BLOCK_SIZE * block_id + offset + record_head_size)
            self.f_handle.write(record_bytes)
        
        # 4. 更新内存中的记录列表（可选）
        # 这里我们不更新内存状态，因为程序可能会继续运行
        # 用户下次打开表时会自动加载最新数据
        
        print(f"Updated {len(record_positions)} records successfully")
        return True