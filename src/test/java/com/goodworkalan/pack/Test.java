/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.pack;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.goodworkalan.pack.Creator;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Opener;
import com.goodworkalan.pack.Pack;

public class Test
{
    @Option(name = "-r", usage = "Replay a file.")
    private File replay;

    @Option(name = "-o", usage = "Output file.")
    private File output;

    @Option(name = "-l", usage = "Log test.")
    private boolean log;

    @Option(name = "-i", usage = "Number of iterations.")
    private int iterations = 1000;

    private static File newFile()
    {
        try
        {
            File file = File.createTempFile("bento", ".bto");
            file.deleteOnExit();
            return file;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
    {
        Test test = new Test();
        CmdLineParser parser = new CmdLineParser(test);
        try
        {
            parser.parseArgument(args);
        }
        catch (CmdLineException e)
        {
            System.err.println(e.getMessage());
            System.err.println("java -jar myprogram.jar [options...] arguments...");
            parser.printUsage(System.err);
            return;
        }
        test.execute();
    }

    public void execute()
    {
        if (replay != null)
        {
            ObjectInputStream in = null;
            try
            {
                in = new ObjectInputStream(new FileInputStream(replay));
            }
            catch (FileNotFoundException e)
            {
                System.out.print("Cannot find file: " + replay.toString());
                System.exit(1);
            }
            catch (IOException e)
            {
                System.out.print("Cannot open file: " + replay.toString() + ", " + e.getMessage());
                System.exit(1);
            }
            Environment environment = new Environment(iterations, 1);
            Stressor stressor = new Stressor(environment, new NullReorder());
            for (;;)
            {
                Operation operation = null;
                try
                {
                    operation = (Operation) in.readObject();
                }
                catch (EOFException e)
                {
                    break;
                }
                catch (IOException e)
                {
                    System.out.print("Cannot read file: " + replay.toString() + " : " + e.getMessage());
                    System.exit(1);
                }
                catch (ClassNotFoundException e)
                {
                    System.out.print("Cannot open file: " + replay.toString() + ", " + e.getMessage());
                    System.exit(1);
                }
                if (operation == null)
                {
                    break;
                }
                if (log)
                {
                    operation.log();
                }
                operation.operate(stressor);
            }
        }
        else if (output != null)
        {
            ObjectOutputStream out = null;
            try
            {
                out = new ObjectOutputStream(new FileOutputStream(output));
            }
            catch (IOException e)
            {
                System.out.print("Cannot open file: " + output.toString() + ", :" + e.getMessage());
                System.exit(1);
            }
            try
            {
                Environment environment = new Environment(iterations, 1);
                Recorder recorder = new FileRecorder(out);
                Stressor stressor = new Stressor(environment, recorder);
                stressor.run();
            }
            catch (RuntimeException e)
            {
                e.printStackTrace();
            }
            finally
            {
                try
                {
                    out.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        else
        {
            Stressor[] stressors = new Stressor[1];
            Environment environment = new Environment(iterations, 1);
            for (int i = 0; i < stressors.length; i++)
            {
                stressors[i] = new Stressor(environment, new ListRecorder());
                new Thread(stressors[i]).start();
            }
        }
    }

    public final static class Stressor
    implements Runnable
    {
        private final Random random = new Random();

        private final Recorder recorder;

        public final Set<Integer> setOfAllocations = new HashSet<Integer>();

        public final List<Operation> listOfRollbackOperations = new ArrayList<Operation>();

        public final List<Allocation> listOfAllocations = new ArrayList<Allocation>();

        public final Map<Long, ByteBuffer> mapOfBlocks = new HashMap<Long, ByteBuffer>();

        public final Environment environment;

        public int allocationCount;

        private Mutator mutator;

        public Stressor(Environment environment, Recorder recorder)
        {
            this.environment = environment;
            this.mutator = environment.pack.mutate();
            this.recorder = recorder;
        }

        public void unallocate(int allocationCount)
        {
            ListIterator<Allocation> allocations = listOfAllocations.listIterator();
            while (allocations.hasNext())
            {
                Allocation candidate = allocations.next();
                if (candidate.allocationCount == allocationCount)
                {
                    allocations.remove();
                    break;
                }
            }
        }

        public void allocate(int blockSize)
        {
            long address = mutator.allocate(blockSize);
            ByteBuffer block = ByteBuffer.allocateDirect(blockSize);
            mutator.read(address, block);
            block.flip();
            Allocation allocation = new Allocation(address, ++allocationCount);
            listOfAllocations.add(allocation);
            int count = (blockSize - 1) / 4;
            block.put((byte) 1);
            for (int i = 0; i < count; i++)
            {
                block.putInt(i);
            }
            block.flip();
            mutator.write(address, block);
            listOfRollbackOperations.add(new Unallocate(allocation.allocationCount));
            setOfAllocations.add(new Integer(allocation.allocationCount));
        }

        public void unfree(Allocation allocation)
        {
            listOfAllocations.add(allocation);
        }

        public void free(int allocationCount)
        {
            ListIterator<Allocation> allocations = listOfAllocations.listIterator();
            while (allocations.hasNext())
            {
                Allocation candidate = allocations.next();
                if (candidate.allocationCount == allocationCount)
                {
                    if (!setOfAllocations.contains(new Integer(candidate.allocationCount)))
                    {
                        listOfRollbackOperations.add(new Unfree(candidate));
                    }
                    allocations.remove();
                    mutator.free(candidate.address);
                    break;
                }
            }
        }

        public void read(long address)
        {
            ByteBuffer block = mutator.read(address);
            boolean forward = block.get() == 1;
            int count = block.remaining() / 4;
            if (forward)
            {
                for (int i = 0; i < count; i++)
                {
                    int got = block.getInt();
                    if (got != i)
                    {
                        throw new IllegalStateException();
                    }
                }
            }
            else
            {
                for (int i = count - 1; i > 0; i--)
                {
                    int got = block.getInt();
                    if (got != i)
                    {
                        throw new IllegalStateException();
                    }
                }
            }
        }

        public void write(long address)
        {
            ByteBuffer block = mutator.read(address);
            boolean forward = block.get() == 1;
            int count = block.remaining() / 4;
            if (forward)
            {
                for (int i = 0; i < count; i++)
                {
                    block.putInt(i);
                }
            }
            else
            {
                for (int i = count - 1; i > 0; i--)
                {
                    block.putInt(i);
                }
            }
            block.flip();
            mutator.write(address, block);
        }

        public void commit()
        {
            mutator.commit();

            listOfRollbackOperations.clear();
            setOfAllocations.clear();
            mutator = environment.pack.mutate();
        }

        public void copacetic()
        {
            environment.pack.copacetic();
        }

        public void rollback()
        {
            Iterator<Operation> rollbacks = listOfRollbackOperations.iterator();
            while (rollbacks.hasNext())
            {
                Operation operation = rollbacks.next();
                operation.operate(this);
            }
            listOfRollbackOperations.clear();
            setOfAllocations.clear();
            mutator.rollback();
            mutator = environment.pack.mutate();
        }

        public void reopen()
        {
            environment.pack.close();
            environment.pack = new Opener().open(environment.file);
            environment.pack.copacetic();
            mutator = environment.pack.mutate();
        }

        public void run()
        {
            boolean reopen = false;
            for (int i = 0; i < environment.iterations; i++)
            {
                Operation operation = null;
                if (listOfAllocations.size() == 0)
                {
                    operation = new Allocate(random);
                }
                else
                {
                    int operationType = random.nextInt(100);
                    if (operationType < 60)
                    {
                        boolean read = random.nextBoolean();
                        if (read)
                        {
                            operation = new Read(this, random);
                        }
                        else
                        {
                            operation = new Write(this, random);
                        }
                    }
                    else if (operationType < 80)
                    {
                        int number = random.nextInt(1000);
                        if (listOfAllocations.size() - number < 0)
                        {
                            operation = new Allocate(random);
                        }
                        else
                        {
                            operation = new Free(this, random);
                        }
                    }
                    else
                    {
                        int terminateType = random.nextInt(10);
                        if (terminateType < 7)
                        {
                            operation = new Commit();
                        }
                        else
                        {
                            operation = new Rollback();
                        }
                        reopen = random.nextInt(5) == 4;
                    }
                }

                recorder.record(operation);
                operation.operate(this);
                if (reopen && environment.threadCount == 1)
                {
                    operation = new Reopen();
                    recorder.record(operation);
                    operation.operate(this);
                    reopen = false;
                }
            }

        }
    }

    private interface Recorder
    {
        public void record(Operation operation);
    }

    public final static class ListRecorder
    implements Recorder
    {
        private final List<Operation> listOfOperations;

        public ListRecorder()
        {
            this.listOfOperations = new ArrayList<Operation>();
        }

        public void record(Operation operation)
        {
            listOfOperations.add(operation);
        }
    }

    public final static class FileRecorder
    implements Recorder
    {
        private final ObjectOutputStream out;

        public FileRecorder(ObjectOutputStream out)
        {
            this.out = out;
        }

        public void record(Operation operation)
        {
            try
            {
                out.writeObject(operation);
            }
            catch (IOException e)
            {
                throw new Error(e);
            }
        }
    }

    public final static class NullReorder
    implements Recorder
    {
        public void record(Operation operation)
        {
        }
    }

    private interface Operation
    extends Serializable
    {
        public void operate(Stressor stressor);

        public void log();
    }

    private final static class Allocate
    implements Operation
    {
        private final static long serialVersionUID = -4610057216068162179L;

        private final int blockSize;

        public Allocate(Random random)
        {
            this.blockSize = random.nextInt(1024 * 4 - 8) + 8;
        }

        public void operate(Stressor stressor)
        {
            stressor.allocate(blockSize);
        }

        public void log()
        {
            System.out.println("stressor.allocate(" + blockSize + ");");
        }
    }

    private final static class Unallocate
    implements Operation
    {
        private final static long serialVersionUID = 20070822L;

        private final int allocationCount;

        public Unallocate(int allocationCount)
        {
            this.allocationCount = allocationCount;
        }

        public void operate(Stressor stressor)
        {
            stressor.unallocate(allocationCount);
        }

        public void log()
        {
        }
    }

    private final static class Free
    implements Operation
    {
        private final static long serialVersionUID = 20070201L;

        private final int allocationCount;

        public Free(Stressor stressor, Random random)
        {
            int index = random.nextInt(stressor.listOfAllocations.size());
            Allocation allocation = stressor.listOfAllocations.get(index);
            this.allocationCount = allocation.allocationCount;
        }

        public void operate(Stressor stressor)
        {
            stressor.free(allocationCount);
        }

        public void log()
        {
            System.out.println("stressor.free(" + allocationCount + ");");
        }
    }

    private final static class Unfree
    implements Operation
    {
        private final static long serialVersionUID = 20070822L;

        private final Allocation allocation;

        public Unfree(Allocation allocation)
        {
            this.allocation = allocation;
        }

        public void operate(Stressor stressor)
        {
            stressor.unfree(allocation);
        }

        public void log()
        {
        }
    }

    private final static class Read
    implements Operation
    {
        private final static long serialVersionUID = 20070201L;

        private final long address;

        public Read(Stressor stressor, Random random)
        {
            int index = random.nextInt(stressor.listOfAllocations.size());
            Allocation allocation = stressor.listOfAllocations.get(index);
            this.address = allocation.address;
        }

        public void operate(Stressor stressor)
        {
            stressor.read(address);
        }

        public void log()
        {
            System.out.println("stressor.read(" + address + "L);");
        }
    }

    private final static class Write
    implements Operation
    {
        private final static long serialVersionUID = 20070201L;

        private final long address;

        public Write(Stressor stressor, Random random)
        {
            int index = random.nextInt(stressor.listOfAllocations.size());
            Allocation allocation = stressor.listOfAllocations.get(index);
            this.address = allocation.address;
        }

        public void operate(Stressor stressor)
        {
            stressor.write(address);
        }

        public void log()
        {
            System.out.println("stressor.write(" + address + "L);");
        }
    }

    private final static class Commit
    implements Operation
    {
        private final static long serialVersionUID = 20070201L;

        public void operate(Stressor stressor)
        {
            stressor.commit();
        }

        public void log()
        {
            System.out.println("stressor.commit();");
        }
    }

    private final static class Rollback
    implements Operation
    {
        private final static long serialVersionUID = 20070201L;

        public void operate(Stressor stressor)
        {
            stressor.rollback();
        }

        public void log()
        {
            System.out.println("stressor.rollback();");
        }
    }

    private final static class Reopen
    implements Operation
    {
        private final static long serialVersionUID = 20070201L;

        public void operate(Stressor stressor)
        {
            stressor.reopen();
        }

        public void log()
        {
            System.out.println("stressor.reopen();");
        }
    }

    public final static class Environment
    {
        public final int threadCount;

        public final int iterations;

        public final File file;

        public Pack pack;

        public Environment(int iterations, int threadCount)
        {
            this.threadCount = threadCount;
            this.iterations = iterations;
            this.file = newFile();
            this.pack = new Creator().create(file);
        }
    }

    private final static class Allocation
    {
        public final int allocationCount;

        public final long address;

        public Allocation(long address, int count)
        {
            this.address = address;
            this.allocationCount = count;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */