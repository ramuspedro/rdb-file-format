package org.project

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder

fun parseRDBFile(file: File) {
    val bytes = file.readBytes()
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    // Read Redis version
    val version = readRedisVersion(buffer)
    println("Redis Version: $version")

    // Read auxiliary fields
    while (true) {
        val type = buffer.get().toInt()
        if (type == 0xFA) { // EOF marker
            break
        }
        if (type == 0xFC){ //Module auxiliary data
            readModuleAux(buffer)
        }
        else if (type == 0xFE) { // Database selector
            val dbNumber = readLengthEncodedInteger(buffer)
            println("Selecting DB: $dbNumber")
        }
        else {
            buffer.position(buffer.position() -1)
            readObject(buffer)
        }
    }
    //Verify checksum
    val fileChecksum = readChecksum(buffer)
    val calculatedChecksum = calculateChecksum(bytes, bytes.size - 8)

    if(fileChecksum == calculatedChecksum) {
        println("Checksum OK")
    } else {
        println("Checksum Mismatch")
    }
}

fun readRedisVersion(buffer: ByteBuffer): String {
    var version = ""
    while (true) {
        val byte = buffer.get().toChar()
        if (byte == '\n') break
        version += byte
    }
    return version.trim()
}
fun readModuleAux(buffer: ByteBuffer){
    val moduleName = readLengthEncodedString(buffer)
    val moduleData = readLengthEncodedString(buffer)
    println("Module Aux: $moduleName - $moduleData")
}
fun readObject(buffer: ByteBuffer){
    val type = buffer.get().toInt()
    when (type) {
        0 -> { // String
            val value = readLengthEncodedString(buffer)
            println("String: $value")
        }
        1 -> { // List
            val length = readLengthEncodedInteger(buffer)
            val list = (0 until length).map{readLengthEncodedString(buffer)}
            println("List: $list")
        }
        2 -> { // Set
            val length = readLengthEncodedInteger(buffer)
            val set = (0 until length).map{readLengthEncodedString(buffer)}.toSet()
            println("Set: $set")
        }
        3 -> { // Sorted Set
            val length = readLengthEncodedInteger(buffer)
            val sortedSet = (0 until length).map{Pair(readLengthEncodedString(buffer), readDouble(buffer))}.toMap()
            println("Sorted Set: $sortedSet")
        }
        4 -> { // Hash
            val length = readLengthEncodedInteger(buffer)
            val hash = (0 until length).map{Pair(readLengthEncodedString(buffer), readLengthEncodedString(buffer))}.toMap()
            println("Hash: $hash")
        }
        9 -> { //Integer
            val value = readLengthEncodedInteger(buffer)
            println("Integer: $value")
        }
        else -> {
            println("Unknown type: $type")
            // Handle unknown type or throw an exception
        }
    }
}
fun readLengthEncodedInteger(buffer: ByteBuffer): Int {
    val firstByte = buffer.get().toInt()
    return when {
        (firstByte and 0xC0) == 0x00 -> firstByte and 0x3F
        (firstByte and 0xC0) == 0x40 -> {
            val nextByte = buffer.get().toInt()
            ((firstByte and 0x3F) shl 8) or nextByte
        }
        firstByte == 0xC0 -> buffer.int
        firstByte == 0xFE -> buffer.long.toInt()
        else -> throw IllegalArgumentException("Invalid length encoding")
    }
}
fun readLengthEncodedString(buffer: ByteBuffer): String {
    val length = readLengthEncodedInteger(buffer)
    val stringBytes = ByteArray(length)
    buffer.get(stringBytes)
    return String(stringBytes)
}
fun readDouble(buffer: ByteBuffer): Double {
    return java.lang.Double.longBitsToDouble(buffer.long)
}
fun readChecksum(buffer: ByteBuffer): Long{
    return buffer.long
}
fun calculateChecksum(bytes: ByteArray, size: Int): Long{
    var checksum:Long = 0
    for(i in 0 until size){
        checksum = (checksum xor (bytes[i].toLong() and 0xFF)) * 0x5bd1e995
    }
    return checksum
}
fun main() {
    val file = File("dump.rdb")
    parseRDBFile(file)
}