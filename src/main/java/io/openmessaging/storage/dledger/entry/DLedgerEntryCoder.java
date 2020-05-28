/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.entry;

import java.nio.ByteBuffer;

public class DLedgerEntryCoder {

    public static void encode(DLedgerEntry entry, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        int size = entry.computSizeInBytes();
        //always put magic on the first position
        byteBuffer.putInt(entry.getMagic());
        byteBuffer.putInt(size);
        byteBuffer.putLong(entry.getIndex());
        byteBuffer.putLong(entry.getTerm());
        byteBuffer.putLong(entry.getPos());
        byteBuffer.putInt(entry.getChannel());
        byteBuffer.putInt(entry.getChainCrc());
        byteBuffer.putInt(entry.getBodyCrc());
        byteBuffer.putInt(entry.getBody().length);
        byteBuffer.put(entry.getBody());
        byteBuffer.flip();
    }

    public static void encodeIndex(long pos, int size, int magic, long index, long term, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        byteBuffer.putInt(magic);
        byteBuffer.putLong(pos);
        byteBuffer.putInt(size);
        byteBuffer.putLong(index);
        byteBuffer.putLong(term);
        byteBuffer.flip();
    }

    public static DLedgerEntry decode(ByteBuffer byteBuffer) {
        return decode(byteBuffer, true);
    }

    public static DLedgerEntry decode(ByteBuffer byteBuffer, boolean readBody) {
        DLedgerEntry entry = new DLedgerEntry();
        entry.setMagic(byteBuffer.getInt());
        entry.setSize(byteBuffer.getInt());
        entry.setIndex(byteBuffer.getLong());
        entry.setTerm(byteBuffer.getLong());
        entry.setPos(byteBuffer.getLong());
        entry.setChannel(byteBuffer.getInt());
        entry.setChainCrc(byteBuffer.getInt());
        entry.setBodyCrc(byteBuffer.getInt());
        int bodySize = byteBuffer.getInt();
        if (readBody && bodySize < entry.getSize()) {
            byte[] body = new byte[bodySize];
            byteBuffer.get(body);
            entry.setBody(body);
        }
        return entry;
    }

    public static void setPos(ByteBuffer byteBuffer, long pos) {
        byteBuffer.mark();
        //这里position的位置加了24个字节，是因为前面setIndexTerm这里写入了24个字节
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        byteBuffer.putLong(pos);
        byteBuffer.reset();
    }

    public static long getPos(ByteBuffer byteBuffer) {
        long pos;
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        pos = byteBuffer.getLong();
        byteBuffer.reset();
        return pos;
    }

    /**
     * 写之前先mark，记录position的位置，写完之后又reset，将position重置到写之前的位置
     * 此时position虽然被重置到写之前的位置，但是数据确实是已经写进入了
     *
     * 猜测：
     * 这个类中，之所以所有的方法写数据前后调用mark和reset，是因为position并不一定是从0开始写的，
     * 可能是此时byteBuffer有多条数据
     */
    public static void setIndexTerm(ByteBuffer byteBuffer, long index, long term, int magic) {
        byteBuffer.mark();
        byteBuffer.putInt(magic);
        //这里跳过的四个字节是：条目日志总长度，包含 Header(协议头) + 消息体，占4字节。
        byteBuffer.position(byteBuffer.position() + 4);
        byteBuffer.putLong(index);
        byteBuffer.putLong(term);
        byteBuffer.reset();
    }

}
