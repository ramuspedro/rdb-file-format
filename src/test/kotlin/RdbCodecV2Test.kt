import com.example.rdb.RDBCodecV2
import com.example.rdb.RdbEntryV2
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class RdbCodecV2Test {

    private val codec = RDBCodecV2()

    @Test
    fun `escrever e ler entradas simples`() {
        val entries = listOf(
            RdbEntryV2("foo", "bar"),
            RdbEntryV2("baz", "qux", ttlMs = 1_710_382_559_637)
        )
        val metadata = mapOf("redis-ver" to "6.0.16")

        val baos = ByteArrayOutputStream()
        codec.write(baos, dbIndex = 0, entries, metadata)

        val data = baos.toByteArray()
        assertTrue(String(data, 0, 9) == "REDIS0011")

        val rdb = codec.read(ByteArrayInputStream(data))
        assertEquals(0, rdb.dbIndex)
        assertEquals("6.0.16", rdb.metadata["redis-ver"])
        assertEquals(2, rdb.entries.size)
        assertEquals(entries[0], rdb.entries[0])
        assertEquals(entries[1], rdb.entries[1])
    }

    @Test
    fun `arquivo RDB vazio`() {
        val baos = ByteArrayOutputStream()
        codec.write(baos, dbIndex = 2, entries = emptyList(), metadata = emptyMap())

        val rdb = codec.read(ByteArrayInputStream(baos.toByteArray()))
        assertEquals(2, rdb.dbIndex)
        assertTrue(rdb.entries.isEmpty())
    }
}
