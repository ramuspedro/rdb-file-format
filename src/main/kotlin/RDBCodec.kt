

import java.io.OutputStream
import java.io.DataOutputStream
import java.io.DataInputStream
import java.io.InputStream
import java.io.PushbackInputStream
import java.io.BufferedInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C

data class RdbEntry(val key: String, val value: String, val ttlMs: Long? = null)
data class RdbFile(val dbIndex: Int, val metadata: Map<String, String>, val entries: List<RdbEntry>)

object RdbCodec {
    private const val MAGIC = "REDIS"
    private const val VERSION = "0011"

    fun write(output: OutputStream, dbIndex: Int, entries: List<RdbEntry>, metadata: Map<String, String> = emptyMap()) {
        val dos = DataOutputStream(output)

        // Header
        dos.writeBytes(MAGIC + VERSION)

        // Metadata
        for ((k, v) in metadata) {
            dos.writeByte(0xFA)
            writeString(dos, k)
            writeString(dos, v)
        }

        // Database section
        dos.writeByte(0xFE)
        writeSize(dos, dbIndex)

        // Hash table info
        dos.writeByte(0xFB)
        writeSize(dos, entries.size)
        writeSize(dos, entries.count { it.ttlMs != null })

        // Entries
        for (entry in entries) {
            entry.ttlMs?.let {
                dos.writeByte(0xFC) // TTL in ms
                val ttlBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(it).array()
                dos.write(ttlBytes)
            }

            dos.writeByte(0x00) // Type: string
            writeString(dos, entry.key)
            writeString(dos, entry.value)
        }

        // End of file
        dos.writeByte(0xFF)

        // CRC64 (simulado com CRC32C por simplicidade)
        val crc = CRC32C().apply { update((MAGIC + VERSION).toByteArray()) }
        dos.writeLong(crc.value)

        dos.flush()
    }

    fun read(input: InputStream): RdbFile {
        val pushback = PushbackInputStream(BufferedInputStream(input), 1)
        val dis = DataInputStream(pushback)

        // Header
        val magic = dis.readNBytes(5).toString(Charsets.US_ASCII)
        require(magic == MAGIC) { "Invalid RDB magic string: $magic" }

        val version = dis.readNBytes(4).toString(Charsets.US_ASCII)
        require(version == VERSION) { "Unsupported RDB version: $version" }

        // Metadata
        val metadata = mutableMapOf<String, String>()
        while (true) {
            val op = dis.readUnsignedByte()
            if (op != 0xFA) {
                pushback.unread(op)
                break
            }
            val key = readString(dis)
            val value = readString(dis)
            metadata[key] = value
        }

        // Database section
        val dbOp = dis.readUnsignedByte()
        require(dbOp == 0xFE) { "Expected DB section (0xFE), got: $dbOp" }

        val dbIndex = readSize(dis)

        val hashInfo = dis.readUnsignedByte()
        require(hashInfo == 0xFB) { "Expected hash table info (0xFB), got: $hashInfo" }

        val totalKeys = readSize(dis)
        val ttlKeys = readSize(dis)

        val entries = mutableListOf<RdbEntry>()

        while (true) {
            val b = dis.readUnsignedByte()
            if (b == 0xFF) break // EOF

            val ttlMs = if (b == 0xFC) {
                val ttlBytes = dis.readNBytes(8)
                ByteBuffer.wrap(ttlBytes).order(ByteOrder.LITTLE_ENDIAN).long
            } else {
                pushback.unread(b)
                null
            }

            val type = dis.readUnsignedByte()
            require(type == 0x00) { "Only string values supported (type 0x00), got: $type" }

            val key = readString(dis)
            val value = readString(dis)

            entries += RdbEntry(key, value, ttlMs)
        }

        dis.skipBytes(8) // Skip checksum (not validated)

        return RdbFile(dbIndex, metadata, entries)
    }

    private fun writeSize(dos: DataOutputStream, size: Int) {
        require(size in 0..63) { "Size encoding only supports values up to 63 in this implementation" }
        dos.writeByte(size)
    }

    private fun writeString(dos: DataOutputStream, s: String) {
        writeSize(dos, s.length)
        dos.writeBytes(s)
    }

    private fun readSize(dis: DataInputStream): Int {
        val b = dis.readUnsignedByte()
        return b and 0x3F
    }

    private fun readString(dis: DataInputStream): String {
        val len = readSize(dis)
        val buf = ByteArray(len)
        dis.readFully(buf)
        return String(buf)
    }
}
