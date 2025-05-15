import com.example.rdb.RdbCodec
import com.example.rdb.RdbEntry
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class RdbCodecTest {

    @Test
    fun `write and read back simple entries`() {
        val entries = listOf(
            RdbEntry("foo", "bar"),
            RdbEntry("baz", "qux", ttlMs = 1_710_382_559_637)
        )
        val metadata = mapOf("redis-ver" to "6.0.16")

        val baos = ByteArrayOutputStream()
        RdbCodec.write(baos, dbIndex = 0, entries, metadata)

        val data = baos.toByteArray()
        // Deve começar com \"REDIS0011\"
        assertTrue(data.sliceArray(0 until 9).toString(Charsets.US_ASCII) == "REDIS0011")

        val rdb = RdbCodec.read(ByteArrayInputStream(data))
        assertEquals(0, rdb.dbIndex)
        assertEquals("6.0.16", rdb.metadata["redis-ver"])
        assertEquals(2, rdb.entries.size)
        assertEquals(entries[0], rdb.entries[0])
        assertEquals(entries[1].copy(), rdb.entries[1])
    }

    @Test
    fun `empty rdb`() {
        val baos = ByteArrayOutputStream()
        RdbCodec.write(baos, dbIndex = 2, entries = emptyList(), metadata = emptyMap())

        val rdb = RdbCodec.read(ByteArrayInputStream(baos.toByteArray()))
        assertEquals(2, rdb.dbIndex)
        assertTrue(rdb.entries.isEmpty())
    }
}