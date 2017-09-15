package com.datatorrent.common.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import static org.junit.Assert.assertEquals;

public class PairSerializableTest
{
  public static final String filename = "target/" + PairSerializableTest.class.getName() + ".bin";

  @Test
  public void testPairSerializability() throws Exception
  {
    Pair<Integer, Integer> pre = new Pair<>(1, 2);

    Kryo kryo = new Kryo();
    FileOutputStream fos = new FileOutputStream(filename);
    Output output = new Output(fos);
    kryo.writeObject(output, pre);
    output.close();

    Input input = new Input(new FileInputStream(filename));
    Pair<Integer, Integer> post = kryo.readObject(input, Pair.class);
    input.close();

    assertEquals("Serialized Deserialized Pair Objects", pre, post);
  }
}
