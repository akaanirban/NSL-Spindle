package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

/**
 * The logical network knows about neighbors and can choose a random one specified by its string.
 */
public interface ILogicalNetwork {

    String ChooseRandomTarget();
}
