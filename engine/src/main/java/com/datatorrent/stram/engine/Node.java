/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.StatsListener.OperatorCommand;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.debug.MuxSink;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;
import com.datatorrent.stram.tuple.EndStreamTuple;
import com.datatorrent.stram.tuple.EndWindowTuple;

/**
 * <p>
 * Abstract Node class.</p>
 *
 * @param <OPERATOR>
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public abstract class Node<OPERATOR extends Operator> implements Component<OperatorContext>, Runnable
{
  /**
   * if the Component is capable of taking only 1 input, call it INPUT.
   */
  public static final String INPUT = "input";
  /**
   * if the Component is capable of providing only 1 output, call it OUTPUT.
   */
  public static final String OUTPUT = "output";
  protected int APPLICATION_WINDOW_COUNT; /* this is write once variable */

  protected int CHECKPOINT_WINDOW_COUNT; /* this is write once variable */

  protected int id;
  protected final HashMap<String, Sink<Object>> outputs;
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile Sink<Object>[] sinks = Sink.NO_SINKS;
  protected boolean alive;
  protected final OPERATOR operator;
  protected final PortMappingDescriptor descriptor;
  public long currentWindowId;
  protected long endWindowEmitTime;
  protected long lastSampleCpuTime;
  protected ThreadMXBean tmb;
  protected HashMap<SweepableReservoir, Long> endWindowDequeueTimes; // end window dequeue time for input ports
  protected Checkpoint checkpoint;
  public int applicationWindowCount;
  public int checkpointWindowCount;
  protected int controlTupleCount;
  protected final boolean stateless;

  public Node(OPERATOR operator)
  {
    this.operator = operator;
    stateless = operator.getClass().isAnnotationPresent(Stateless.class);
    outputs = new HashMap<String, Sink<Object>>();

    descriptor = new PortMappingDescriptor();
    Operators.describe(operator, descriptor);

    endWindowDequeueTimes = new HashMap<SweepableReservoir, Long>();
    tmb = ManagementFactory.getThreadMXBean();
  }

  public Operator getOperator()
  {
    return operator;
  }

  @Override
  public void setup(OperatorContext context)
  {
    shutdown = false;
    operator.setup(context);
//    this is where the ports should be setup but since the
//    portcontext is not available here, we are doing it in
//    StramChild. In future version, we should move that code here
//    for (InputPort<?> port : descriptor.inputPorts.values()) {
//      port.setup(null);
//    }
//
//    for (OutputPort<?> port : descriptor.outputPorts.values()) {
//      port.setup(null);
//    }
  }

  @Override
  public void teardown()
  {
    for (PortContextPair<InputPort<?>> pcpair : descriptor.inputPorts.values()) {
      pcpair.component.teardown();
    }

    for (PortContextPair<OutputPort<?>> pcpair : descriptor.outputPorts.values()) {
      pcpair.component.teardown();
    }

    operator.teardown();
  }

  public PortMappingDescriptor getPortMappingDescriptor()
  {
    return descriptor;
  }

  public void connectOutputPort(String port, final Sink<Object> sink)
  {
    PortContextPair<OutputPort<?>> outputPort = descriptor.outputPorts.get(port);
    if (outputPort != null) {
      if (sink == null) {
        outputPort.component.setSink(null);
        outputs.remove(port);
      }
      else {
        outputPort.component.setSink(sink);
        outputs.put(port, sink);
      }
    }
  }

  public abstract void connectInputPort(String port, final SweepableReservoir reservoir);

  @SuppressWarnings({"unchecked"})
  public void addSinks(Map<String, Sink<Object>> sinks)
  {
    boolean changes = false;
    for (Entry<String, Sink<Object>> e : sinks.entrySet()) {
      /* make sure that we ignore all the input ports */
      PortContextPair<OutputPort<?>> pcpair = descriptor.outputPorts.get(e.getKey());
      if (pcpair == null) {
        continue;
      }
      changes = true;

      Sink<Object> ics = outputs.get(e.getKey());
      if (ics == null) {
        pcpair.component.setSink(e.getValue());
        outputs.put(e.getKey(), e.getValue());
        changes = true;
      }
      else if (ics instanceof MuxSink) {
        ((MuxSink)ics).add(e.getValue());
      }
      else {
        MuxSink muxSink = new MuxSink(ics, e.getValue());
        pcpair.component.setSink(muxSink);
        outputs.put(e.getKey(), muxSink);
        changes = true;
      }
    }

    if (changes) {
      activateSinks();
    }
  }

  public void removeSinks(Map<String, Sink<Object>> sinks)
  {
    boolean changes = false;
    for (Entry<String, Sink<Object>> e : sinks.entrySet()) {
      /* make sure that we ignore all the input ports */
      PortContextPair<OutputPort<?>> pcpair = descriptor.outputPorts.get(e.getKey());
      if (pcpair == null) {
        continue;
      }

      Sink<Object> ics = outputs.get(e.getKey());
      if (ics == e.getValue()) {
        pcpair.component.setSink(null);
        outputs.remove(e.getKey());
        changes = true;
      }
      else if (ics instanceof MuxSink) {
        MuxSink ms = (MuxSink)ics;
        ms.remove(e.getValue());
        Sink<Object>[] sinks1 = ms.getSinks();
        if (sinks1.length == 0) {
          pcpair.component.setSink(null);
          outputs.remove(e.getKey());
          changes = true;
        }
        else if (sinks1.length == 1) {
          pcpair.component.setSink(sinks1[0]);
          outputs.put(e.getKey(), sinks1[0]);
          changes = true;
        }
      }
    }

    if (changes) {
      activateSinks();
    }
  }

  protected OperatorContext context;
  protected ProcessingMode PROCESSING_MODE;
  protected volatile boolean shutdown;

  public void shutdown()
  {
    shutdown = true;

    synchronized (this) {
      alive = false;
    }

    if (context == null) {
      logger.warn("Shutdown requested when context is not available!");
    }
    else {
      context.request(new OperatorCommand()
      {
        @Override
        public void execute(Operator operator, int operatorId, long windowId) throws IOException
        {
          alive = false;
        }

      });
    }
  }

  @Override
  public String toString()
  {
    return String.valueOf(getId());
  }

  protected void emitEndStream()
  {
    // logger.debug("{} sending EndOfStream", this);
    /*
     * since we are going away, we should let all the downstream operators know that.
     */
    EndStreamTuple est = new EndStreamTuple(currentWindowId);
    for (final Sink<Object> output : outputs.values()) {
      output.put(est);
    }
    controlTupleCount++;
  }

  protected void emitEndWindow()
  {
    EndWindowTuple ewt = new EndWindowTuple(currentWindowId);
    for (int s = sinks.length; s-- > 0;) {
      sinks[s].put(ewt);
    }
    controlTupleCount++;
  }

  protected void handleRequests(long windowId)
  {
    /*
     * we prefer to cater to requests at the end of the window boundary.
     */
    try {
      BlockingQueue<OperatorCommand> requests = context.getRequests();
      int size;
      if ((size = requests.size()) > 0) {
        while (size-- > 0) {
          //logger.debug("endwindow: " + t.getWindowId() + " lastprocessed: " + context.getLastProcessedWindowId());
          requests.remove().execute(operator, context.getId(), windowId);
        }
      }
    }
    catch (Error er) {
      throw er;
    }
    catch (RuntimeException re) {
      throw re;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void reportStats(ContainerStats.OperatorStats stats, long windowId)
  {
    stats.outputPorts = new ArrayList<ContainerStats.OperatorStats.PortStats>();
    for (Entry<String, Sink<Object>> e : outputs.entrySet()) {
      ContainerStats.OperatorStats.PortStats portStats = new ContainerStats.OperatorStats.PortStats(e.getKey());
      portStats.tupleCount = e.getValue().getCount(true) - controlTupleCount;
      controlTupleCount = 0;
      portStats.endWindowTimestamp = endWindowEmitTime;
      stats.outputPorts.add(portStats);
    }

    long currentCpuTime = tmb.getCurrentThreadCpuTime();
    stats.cpuTimeUsed = currentCpuTime - lastSampleCpuTime;
    lastSampleCpuTime = currentCpuTime;

    if (checkpoint != null) {
      stats.checkpoint = checkpoint;
      checkpoint = null;
    }

    context.report(stats, windowId);
  }

  protected void activateSinks()
  {
    int size = outputs.size();
    if (size == 0) {
      sinks = Sink.NO_SINKS;
    }
    else {
      @SuppressWarnings("unchecked")
      Sink<Object>[] newSinks = (Sink<Object>[])Array.newInstance(Sink.class, size);
      for (Sink<Object> s : outputs.values()) {
        newSinks[--size] = s;
      }

      sinks = newSinks;
    }
  }

  protected void deactivateSinks()
  {
    sinks = Sink.NO_SINKS;
  }

  public static void storeOperator(OutputStream stream, Operator operator) throws IOException
  {
    Output output = new Output(4096, Integer.MAX_VALUE);
    output.setOutputStream(stream);
    final Kryo k = new Kryo();
    k.writeClassAndObject(output, operator);
    output.flush();
  }

  public static Operator retrieveOperator(InputStream stream)
  {
    final Kryo k = new Kryo();
    k.setClassLoader(Thread.currentThread().getContextClassLoader());
    Input input = new Input(stream);
    return (Operator)k.readClassAndObject(input);
  }

  void checkpoint(long windowId)
  {
    if (!stateless) {
      StorageAgent ba = context.getAttributes().get(OperatorContext.STORAGE_AGENT);
      if (ba != null) {
        try {
          OutputStream stream = ba.getSaveStream(id, windowId);
          try {
            Node.storeOperator(stream, operator);
          }
          finally {
            stream.close();
          }
        }
        catch (IOException ie) {
          try {
            logger.warn("Rolling back checkpoint {} for Operator {} due to the exception {}",
                        new Object[] {Codec.getStringWindowId(windowId), operator, ie});
            ba.delete(id, windowId);
          }
          catch (IOException ex) {
            logger.warn("Error while rolling back checkpoint", ex);
          }
          throw new RuntimeException(ie);
        }
      }
    }

    checkpoint = new Checkpoint(windowId, applicationWindowCount, checkpointWindowCount);
    if (operator instanceof CheckpointListener) {
      ((CheckpointListener)operator).checkpointed(windowId);
    }
  }

  @SuppressWarnings("unchecked")
  public static Node<?> retrieveNode(InputStream stream, OperatorDeployInfo.OperatorType type)
  {
    Operator operator = retrieveOperator(stream);
    logger.debug("type={}, operator class={}", type, operator.getClass());

    Node<?> node;
    if (operator instanceof InputOperator && type == OperatorDeployInfo.OperatorType.INPUT) {
      node = new InputNode((InputOperator)operator);
    }
    else if (operator instanceof Unifier && type == OperatorDeployInfo.OperatorType.UNIFIER) {
      node = new UnifierNode((Unifier<Object>)operator);
    }
    else if (type == OperatorDeployInfo.OperatorType.OIO) {
      node = new OiONode(operator);
    }
    else {
      node = new GenericNode(operator);
    }

    return node;
  }

  /**
   * @return the id
   */
  public int getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(int id)
  {
    if (this.id == 0) {
      this.id = id;
    }
    else {
      throw new RuntimeException("Id cannot be changed from " + this.id + " to " + id);
    }
  }

  public OperatorContext getContext()
  {
    return context;
  }

  @SuppressWarnings("unchecked")
  public void activate(OperatorContext context)
  {
    this.context = context;
    alive = true;
    APPLICATION_WINDOW_COUNT = context.getValue(OperatorContext.APPLICATION_WINDOW_COUNT);
    CHECKPOINT_WINDOW_COUNT = context.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT);

    PROCESSING_MODE = context.getValue(OperatorContext.PROCESSING_MODE);
    if (PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE && CHECKPOINT_WINDOW_COUNT != 1) {
      logger.warn("Ignoring CHECKPOINT_WINDOW_COUNT attribute in favor of EXACTLY_ONCE processing mode");
      CHECKPOINT_WINDOW_COUNT = 1;
    }

    activateSinks();
    if (operator instanceof ActivationListener) {
      ((ActivationListener<OperatorContext>)operator).activate(context);
    }

    /*
     * If there were any requests which needed to be executed before the operator started
     * its normal execution, execute those requests now - e.g. Restarting the operator
     * recording for the operators which failed while recording and being replaced.
     */
    handleRequests(currentWindowId);
  }

  public void deactivate()
  {
    if (operator instanceof ActivationListener) {
      ((ActivationListener<?>)operator).deactivate();
    }

    if (!shutdown && !alive) {
      emitEndStream();
    }

    deactivateSinks();
    this.context = null;
  }

  private static final Logger logger = LoggerFactory.getLogger(Node.class);
}
