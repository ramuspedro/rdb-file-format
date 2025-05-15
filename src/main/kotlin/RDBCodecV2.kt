package com.example.rdb

import java.io.*
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * Serviço para leitura e escrita de arquivos RDB no formato Redis 11.
 */
class RDBCodecV2 {

    companion object {
        private const val MAGIC = "REDIS"
        private const val VERSION = "0011"
    }

    /**
     * Escreve os dados fornecidos em um arquivo RDB.
     *
     * @param outputStream Fluxo de saída para escrita.
     * @param dbIndex Índice do banco de dados.
     * @param entries Lista de entradas a serem escritas.
     * @param metadata Metadados opcionais.
     */
    fun write(outputStream: OutputStream, dbIndex: Int, entries: List<RdbEntryV2>, metadata: Map<String, String> = emptyMap()) {
        val dos = DataOutputStream(outputStream)

        // Escreve o cabeçalho
        dos.writeBytes(MAGIC + VERSION)

        // Escreve os metadados
        for ((key, value) in metadata) {
            dos.writeByte(0xFA)
            writeString(dos, key)
            writeString(dos, value)
        }

        // Início da seção do banco de dados
        dos.writeByte(0xFE)
        writeSize(dos, dbIndex)

        // Informações da tabela hash (tamanhos fictícios)
        dos.writeByte(0xFB)
        writeSize(dos, entries.size) // Tamanho da tabela de chaves
        writeSize(dos, entries.count { it.ttlMs != null }) // Tamanho da tabela de expirados

        // Escreve as entradas
        for (entry in entries) {
            entry.ttlMs?.let {
                dos.writeByte(0xFC) // TTL em milissegundos
                val buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(it).array()
                dos.write(buffer)
            }

            dos.writeByte(0x00) // Tipo de valor: string
            writeString(dos, entry.key)
            writeString(dos, entry.value)
        }

        // Final do arquivo
        dos.writeByte(0xFF)

        // Calcula e escreve o checksum CRC64
        val crc = CRC32()
        val data = (MAGIC + VERSION).toByteArray()
        crc.update(data)
        dos.writeLong(crc.value)

        dos.flush()
    }

    /**
     * Lê um arquivo RDB e retorna sua representação em memória.
     *
     * @param inputStream Fluxo de entrada para leitura.
     * @return Representação do arquivo RDB.
     */
    fun read(inputStream: InputStream): RdbFile {
        val dis = PushbackInputStream(inputStream)

        // Lê e valida o cabeçalho
        val magicBytes = ByteArray(5)
        dis.read(magicBytes)
        val magic = String(magicBytes)
        require(magic == MAGIC) { "Magic inválido: $magic" }

        val versionBytes = ByteArray(4)
        dis.read(versionBytes)
        val version = String(versionBytes)
        require(version == VERSION) { "Versão RDB não suportada: $version" }

        // Lê os metadados
        val metadata = mutableMapOf<String, String>()
        while (true) {
            val b = dis.read()
            if (b == 0xFA) {
                val key = readString(dis)
                val value = readString(dis)
                metadata[key] = value
            } else {
                dis.unread(b)
                break
            }
        }

        // Lê a seção do banco de dados
        val op = dis.read()
        require(op == 0xFE) { "Esperado início da seção do banco de dados, mas encontrado: $op" }
        val dbIndex = readSize(dis)

        // Lê informações da tabela hash (ignoradas neste exemplo)
        readSize(dis)
        readSize(dis)

        val entries = mutableListOf<RdbEntryV2>()
        while (true) {
            val next = dis.read()
            if (next == 0xFF) break

            val ttl = if (next == 0xFC) {
                val buffer = ByteArray(8)
                dis.read(buffer)
                ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).long
            } else {
                dis.unread(next)
                null
            }

            val type = dis.read()
            require(type == 0x00) { "Tipo de valor não suportado: $type" }

            val key = readString(dis)
            val value = readString(dis)
            entries += RdbEntryV2(key, value, ttl)
        }

        // Ignora o checksum
        dis.skip(8)

        return RdbFile(dbIndex, metadata, entries)
    }

    private fun writeSize(dos: DataOutputStream, size: Int) {
        require(size shr 6 == 0) { "Tamanho >63 não suportado" }
        dos.writeByte(size)
    }

    private fun writeString(dos: DataOutputStream, s: String) {
        writeSize(dos, s.length)
        dos.writeBytes(s)
    }

    private fun readSize(dis: PushbackInputStream): Int {
        val b = dis.read()
        return b and 0x3F
    }

    private fun readString(dis: PushbackInputStream): String {
        val length = readSize(dis)
        val buffer = ByteArray(length)
        dis.read(buffer)
        return String(buffer)
    }
}

/**
 * Representa uma entrada no arquivo RDB.
 *
 * @param key Chave da entrada.
 * @param value Valor associado à chave.
 * @param ttlMs Tempo de vida em milissegundos (opcional).
 */
data class RdbEntryV2(val key: String, val value: String, val ttlMs: Long? = null)

/**
 * Representa o conteúdo de um arquivo RDB.
 *
 * @param dbIndex Índice do banco de dados.
 * @param metadata Metadados associados.
 * @param entries Lista de entradas no banco de dados.
 */
data class RdbFile(val dbIndex: Int, val metadata: Map<String, String>, val entries: List<RdbEntryV2>)
