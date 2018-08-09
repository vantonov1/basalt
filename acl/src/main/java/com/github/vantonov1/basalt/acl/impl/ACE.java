package com.github.vantonov1.basalt.acl.impl;

class ACE {
    public String nodeId;
    public String authorityId;
    public int mask;

    public ACE(String nodeId, String authorityId, int mask) {
        this.nodeId = nodeId;
        this.authorityId = authorityId;
        this.mask = mask;
    }
}
