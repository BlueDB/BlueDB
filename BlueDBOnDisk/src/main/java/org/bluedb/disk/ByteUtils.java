package org.bluedb.disk;

import java.io.ByteArrayOutputStream;

import org.nustaq.serialization.FSTClazzNameRegistry;

public class ByteUtils {

	public static byte[] replaceAllBytes(byte[] source, byte[] target, byte[] replacement) {
		if(isByteArrayEmpty(source) || isByteArrayEmpty(target) || replacement == null) {
			return source;
		}
		
		ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
		int sourceIndex = 0;
		int matchIndexOffset = 0;
		
		int matchIndex;
		while((matchIndex = indexOf(source, target, matchIndexOffset)) != -1) {
			int bytesBeforeMatch = matchIndex - sourceIndex;
			byteBuffer.write(source, sourceIndex, bytesBeforeMatch); //write bytes up to this match
			byteBuffer.write(replacement, 0, replacement.length); //write replacing bytes instead of target bytes
			sourceIndex += bytesBeforeMatch + target.length;
			matchIndexOffset = matchIndex + 1;
		}
		
		int remainingBytesToCopy = source.length - sourceIndex;
		if(remainingBytesToCopy > 0) {
			byteBuffer.write(source, sourceIndex, remainingBytesToCopy);
		}
		
		return byteBuffer.toByteArray();
	}

	public static byte[] replaceClassPathBytes(byte[] source, String oldClasspath, String newClasspath) {
		byte[] target = oldClasspath != null ? oldClasspath.getBytes() : null;
		byte[] replacement = newClasspath != null ? newClasspath.getBytes() : null;
		
		if(isByteArrayEmpty(source) || isByteArrayEmpty(target) || replacement == null) {
			return source;
		}
		
		ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
		int sourceIndex = 0;
		int matchIndexOffset = 0;
		int diffBetweenTargetAndReplacementLength = replacement.length - target.length;
		
		int matchIndex;
		while((matchIndex = indexOf(source, target, matchIndexOffset)) != -1) {
			int bytesUsedForSize = determineNumberOfBytesUsedForFstClassSize(source, matchIndex);
			
			int bytesBeforeMatch = matchIndex - sourceIndex;
			byteBuffer.write(source, sourceIndex, bytesBeforeMatch - bytesUsedForSize); //write bytes up to the size byte which is the byte preceding the match
			sourceIndex += bytesBeforeMatch - bytesUsedForSize; //update index accordingly
			
			int size = readFstClassSize(source, sourceIndex);
			size += diffBetweenTargetAndReplacementLength;
			writeFstClassSize(byteBuffer, size);  //write the new size now that the class has been changed
			sourceIndex += bytesUsedForSize; //skip original length of size
			
			byteBuffer.write(replacement, 0, replacement.length); //write replacing bytes instead of target bytes
			sourceIndex += target.length; //skip target bytes
			
			matchIndexOffset = matchIndex + 1;
		}
		
		int remainingBytesToCopy = source.length - sourceIndex;
		if(remainingBytesToCopy > 0) {
			byteBuffer.write(source, sourceIndex, remainingBytesToCopy);
		}
		
		return byteBuffer.toByteArray();
	}

	public static int indexOf(byte[] array, byte[] target, int offset) {
	    if (target.length == 0) {
	      return -1;
	    }

	    outer:
	    for (int i = offset; i < array.length - target.length + 1; i++) {
	      for (int j = 0; j < target.length; j++) {
	        if (array[i + j] != target[j]) {
	          continue outer;
	        }
	      }
	      return i;
	    }
	    return -1;
	  }
	
	public static boolean isByteArrayEmpty(byte[] bytes) {
		return bytes == null || bytes.length == 0;
	}
	
	public static int determineNumberOfBytesUsedForFstClassSize(byte[] source, int matchIndex) {
		/*
		 * Return
		 * 1 if [0 (FSTObjectOutput.OBJECT), something < FSTClazzNameRegistry.LOWEST_CLZ_ID, byte1]
		 * 3 if [0 (FSTObjectOutput.OBJECT), something < FSTClazzNameRegistry.LOWEST_CLZ_ID, -128, byte1, byte2]
		 * 5 if [0 (FSTObjectOutput.OBJECT), something < FSTClazzNameRegistry.LOWEST_CLZ_ID, -127, byte1, byte2, byte3, byte4]
		 */
		if(matchIndex-6 >= 0 && 
//				source[matchIndex-6] == FSTObjectOutput.OBJECT && 
				source[matchIndex-6] < FSTClazzNameRegistry.LOWEST_CLZ_ID &&
				source[matchIndex-5] == -127) {
			return 5;
		} else if(matchIndex-4 >= 0 && 
//				source[matchIndex-4] == FSTObjectOutput.OBJECT && 
				source[matchIndex-4] < FSTClazzNameRegistry.LOWEST_CLZ_ID &&
				source[matchIndex-3] == -128) {
			return 3;
		} else { //if(matchIndex-2 >= 0 && 
//				source[matchIndex-2] == FSTObjectOutput.OBJECT && 
//				source[matchIndex-2] < FSTClazzNameRegistry.LOWEST_CLZ_ID &&
//				source[matchIndex-1] > -127 && source[matchIndex-1] <= 127) {
			return 1;
		}
	}

	public static int readFstClassSize(byte[] source, int index) {
        final byte head = source[index];
        // -128 = short byte, -127 == 4 byte
        if (head > -127 && head <= 127) {
            return head;
        }
        if (head == -128) {
            int ch1 = (source[index+1] + 256) & 0xff;
            int ch2 = (source[index+2] + 256) & 0xff;
            return (short) ((ch2 << 8) + (ch1 << 0));
        } else {
            int ch1 = (source[index+1] + 256) & 0xff;
            int ch2 = (source[index+2] + 256) & 0xff;
            int ch3 = (source[index+3] + 256) & 0xff;
            int ch4 = (source[index+4] + 256) & 0xff;
            int res = (ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0);
            return res;
        }
    }

	public static void writeFstClassSize(ByteArrayOutputStream buffer, int size) {
        // -128 = short byte, -127 == 4 byte
        if (size > -127 && size <= 127) {
        	buffer.write(size);
        } else if (size >= Short.MIN_VALUE && size <= Short.MAX_VALUE) {
        	buffer.write(-128);
        	buffer.write(size >>> 0);
        	buffer.write(size >>> 8);
        } else {
        	buffer.write(-127);
        	buffer.write((size >>> 0) & 0xFF);
        	buffer.write((size >>>  8) & 0xFF);
        	buffer.write((size >>> 16) & 0xFF);
        	buffer.write((size >>> 24) & 0xFF);
        }
    }
}
