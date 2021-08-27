/*
 * The conceptual implementation of the Spark-MPI plugin 
 * developed after the OMPI ODLS default component by
 * renaming and changing the prte_odls_default_launch_local_procs &
 * odls_default_fork_local_proc methods
 */

/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file:
 */

#ifndef PRTE_ODLS_SPARKMPI_H
#define PRTE_ODLS_SPARKMPI_H

#include "prte_config.h"

#include "src/mca/mca.h"

#include "src/mca/odls/odls.h"
#include "src/mca/odls/base/odls_private.h"

BEGIN_C_DECLS

/*
 * Module open / close
 */
int prte_odls_blaze_component_open(void);
int prte_odls_blaze_component_close(void);
int prte_odls_blaze_component_query(prte_mca_base_module_t **module, int *priority);

/*
 * ODLS Default module
 */
extern prte_odls_base_module_t prte_odls_blaze_module;
PRTE_MODULE_EXPORT extern prte_odls_base_component_t mca_odls_blaze_component;
PRTE_EXPORT int prte_odls_blaze_default_kill_local_procs(prte_pointer_array_t *procs,
                                             prte_odls_base_kill_local_fn_t kill_local);
END_C_DECLS

#endif /* PRTE_ODLS_SPARKMPI_H */
