import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, Input, OnChanges, SimpleChanges } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { catchError, finalize, of, timeout } from 'rxjs';
import { ApiService } from '../../core/api.service';

type AssignmentRow = {
  role_id: string;
  faculty_ids: string[];
  academic_program_ids: string[];
  is_active: boolean;
  faculty_search: string;
  program_search: string;
};

@Component({
  selector: 'app-security-admin-panel',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './security-admin-panel.component.html',
  styleUrl: './security-admin-panel.component.css',
})
export class SecurityAdminPanelComponent implements OnChanges {
  @Input() showHeader = true;
  @Input() activeView: 'users' | 'roles' = 'users';

  users: any[] = [];
  roles: any[] = [];
  permissions: any[] = [];
  faculties: any[] = [];
  academicPrograms: any[] = [];
  availableRoles: any[] = [];

  selectedUserId = '';
  selectedRoleId = '';
  isLoading = false;
  isSavingUser = false;
  isSavingRole = false;
  feedback = '';
  errorMessage = '';

  userForm = this.emptyUserForm();
  userAssignments: AssignmentRow[] = [this.emptyAssignment()];

  roleForm = this.emptyRoleForm();
  rolePermissionIds = new Set<string>();
  private pendingLoads = 0;

  constructor(
    private readonly api: ApiService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnChanges(changes: SimpleChanges) {
    if (changes['activeView']) {
      this.loadData();
    }
  }

  loadData() {
    this.errorMessage = '';
    if (this.activeView === 'users') {
      this.beginLoad();
      this.api
        .listAdminUsers()
        .pipe(
          timeout(15000),
          catchError((error) => {
            this.errorMessage = this.buildLoadError('usuarios', error);
            return of([]);
          }),
          finalize(() => this.endLoad()),
        )
        .subscribe((users) => {
          this.users = Array.isArray(users) ? users : [];
          this.syncSelectionsAfterReload();
          this.cdr.detectChanges();
        });

      this.beginLoad();
      this.api
        .getAdminScopeCatalog()
        .pipe(
          timeout(15000),
          catchError((error) => {
            this.errorMessage = this.buildLoadError('catalogos de alcance', error);
            return of({
              faculties: [],
              academic_programs: [],
              roles: [],
            });
          }),
          finalize(() => this.endLoad()),
        )
        .subscribe((scopeCatalog) => {
          this.faculties = Array.isArray(scopeCatalog?.faculties) ? scopeCatalog.faculties : [];
          this.academicPrograms = Array.isArray(scopeCatalog?.academic_programs)
            ? scopeCatalog.academic_programs
            : [];
          this.availableRoles = Array.isArray(scopeCatalog?.roles) ? scopeCatalog.roles : [];
          this.cdr.detectChanges();
        });
      return;
    }

    this.beginLoad();
    this.api
      .listAdminRoles()
      .pipe(
        timeout(15000),
        catchError((error) => {
          this.errorMessage = this.buildLoadError('roles', error);
          return of([]);
        }),
        finalize(() => this.endLoad()),
      )
      .subscribe((roles) => {
        this.roles = Array.isArray(roles) ? roles : [];
        this.syncSelectionsAfterReload();
        this.cdr.detectChanges();
      });

    this.beginLoad();
    this.api
      .listAdminPermissions()
      .pipe(
        timeout(15000),
        catchError((error) => {
          this.errorMessage = this.buildLoadError('privilegios', error);
          return of([]);
        }),
        finalize(() => this.endLoad()),
      )
      .subscribe((permissions) => {
        this.permissions = Array.isArray(permissions) ? permissions : [];
        this.syncSelectionsAfterReload();
        this.cdr.detectChanges();
      });
  }

  selectUser(user: any) {
    this.selectedUserId = user.id;
    this.userForm = {
      id: user.id,
      username: user.username ?? '',
      display_name: user.display_name ?? '',
      email: user.email ?? '',
      password: '',
      is_active: user.is_active ?? true,
    };
    this.userAssignments =
      user.assignments?.length > 0 ? this.mapAssignmentsToRows(user.assignments) : [this.emptyAssignment()];
    this.feedback = '';
    this.errorMessage = '';
  }

  createNewUser() {
    this.selectedUserId = '';
    this.userForm = this.emptyUserForm();
    this.userAssignments = [this.emptyAssignment()];
  }

  addAssignment() {
    this.userAssignments = [...this.userAssignments, this.emptyAssignment()];
  }

  removeAssignment(index: number) {
    this.userAssignments = this.userAssignments.filter((_, rowIndex) => rowIndex !== index);
    if (this.userAssignments.length === 0) {
      this.userAssignments = [this.emptyAssignment()];
    }
  }

  saveUser() {
    if (!this.userForm.username.trim() || !this.userForm.display_name.trim()) {
      this.errorMessage = 'Completa username y nombre visible.';
      return;
    }
    if (!this.selectedUserId && !this.userForm.password.trim()) {
      this.errorMessage = 'La password es obligatoria al crear un usuario.';
      return;
    }
    const validAssignments = this.userAssignments.filter((assignment) => assignment.role_id);
    if (validAssignments.length === 0) {
      this.errorMessage = 'Agrega al menos una asignacion de rol.';
      return;
    }
    const expandedAssignments = validAssignments.flatMap((assignment) =>
      this.expandAssignmentRow(assignment),
    );
    if (expandedAssignments.length === 0) {
      this.errorMessage = 'Selecciona al menos una facultad o programa valido.';
      return;
    }

    this.isSavingUser = true;
    this.errorMessage = '';
    this.feedback = '';
    const payload = {
      username: this.userForm.username.trim(),
      display_name: this.userForm.display_name.trim(),
      email: this.userForm.email.trim() || null,
      password: this.userForm.password.trim() || undefined,
      is_active: this.userForm.is_active,
    };
    const request$ = this.selectedUserId
      ? this.api.updateAdminUser(this.selectedUserId, payload)
      : this.api.createAdminUser(payload);

    request$.subscribe({
      next: (user) => {
        const userId = user.id;
        this.api
          .replaceAdminUserAssignments(userId, {
            assignments: expandedAssignments,
          })
          .subscribe({
            next: () => {
              this.feedback = 'Usuario y asignaciones guardados.';
              this.isSavingUser = false;
              this.selectedUserId = userId;
              this.loadData();
            },
            error: (error) => {
              this.isSavingUser = false;
              this.errorMessage = error?.error?.message ?? 'No se pudieron guardar las asignaciones.';
            },
          });
      },
      error: (error) => {
        this.isSavingUser = false;
        this.errorMessage = error?.error?.message ?? 'No se pudo guardar el usuario.';
      },
    });
  }

  selectRole(role: any) {
    this.selectedRoleId = role.id;
    this.roleForm = {
      id: role.id,
      code: role.code ?? '',
      name: role.name ?? '',
      is_active: role.is_active ?? true,
      is_editable: role.is_editable ?? true,
    };
    this.rolePermissionIds = this.normalizeRolePermissionIds(role.permissions ?? []);
    this.feedback = '';
    this.errorMessage = '';
  }

  createNewRole() {
    this.selectedRoleId = '';
    this.roleForm = this.emptyRoleForm();
    this.rolePermissionIds = new Set<string>();
  }

  toggleRolePermission(permissionId: string, checked: boolean) {
    const permission = this.permissions.find((item) => item.id === permissionId);
    if (!permission) {
      return;
    }
    const next = new Set(this.rolePermissionIds);
    if (checked) {
      next.add(permissionId);
      if (permission.parent_window_code) {
        const parent = this.permissions.find((item) => item.code === permission.parent_window_code);
        if (parent?.id) {
          next.add(parent.id);
        }
      }
    } else {
      next.delete(permissionId);
      if (permission.type === 'WINDOW') {
        this.permissions
          .filter((item) => item.parent_window_code === permission.code)
          .forEach((child) => next.delete(child.id));
      }
    }
    this.rolePermissionIds = next;
  }

  permissionLabel(permission: any) {
    return permission?.display_name || permission?.description || permission?.code || 'Privilegio';
  }

  permissionSecondaryLabel(permission: any) {
    return permission?.code || '';
  }

  get windowPermissionGroups() {
    const windows = this.permissions
      .filter((item) => item.type === 'WINDOW')
      .sort((a, b) => Number(a.sort_order ?? 0) - Number(b.sort_order ?? 0));
    return windows.map((windowPermission) => ({
      windowPermission,
      actions: this.permissions
        .filter((item) => item.parent_window_code === windowPermission.code)
        .sort((a, b) => Number(a.sort_order ?? 0) - Number(b.sort_order ?? 0)),
    }));
  }

  get globalActionPermissions() {
    return this.permissions
      .filter((item) => item.type === 'ACTION' && !item.parent_window_code)
      .sort((a, b) => Number(a.sort_order ?? 0) - Number(b.sort_order ?? 0));
  }

  get selectedRolePermissionSummary() {
    const selectedPermissions = this.permissions.filter((item) => this.rolePermissionIds.has(item.id));
    return {
      windowCount: selectedPermissions.filter((item) => item.type === 'WINDOW').length,
      actionCount: selectedPermissions.filter((item) => item.type === 'ACTION').length,
    };
  }

  saveRole() {
    if (!this.roleForm.code.trim() || !this.roleForm.name.trim()) {
      this.errorMessage = 'Completa codigo y nombre del rol.';
      return;
    }
    this.isSavingRole = true;
    this.errorMessage = '';
    this.feedback = '';
    const payload = {
      code: this.roleForm.code.trim(),
      name: this.roleForm.name.trim(),
      is_active: this.roleForm.is_active,
      is_editable: this.roleForm.is_editable,
    };
    const request$ = this.selectedRoleId
      ? this.api.updateAdminRole(this.selectedRoleId, payload)
      : this.api.createAdminRole(payload);

    request$.subscribe({
      next: (role) => {
        const roleId = role.id;
        this.api
          .replaceRolePermissions(roleId, {
            permission_ids: [...this.rolePermissionIds],
          })
          .subscribe({
            next: () => {
              this.feedback = 'Rol y privilegios guardados.';
              this.isSavingRole = false;
              this.selectedRoleId = roleId;
              this.loadData();
            },
            error: (error) => {
              this.isSavingRole = false;
              this.errorMessage = error?.error?.message ?? 'No se pudieron guardar los privilegios.';
            },
          });
      },
      error: (error) => {
        this.isSavingRole = false;
        this.errorMessage = error?.error?.message ?? 'No se pudo guardar el rol.';
      },
      });
  }

  filteredFaculties(assignment: AssignmentRow) {
    const query = this.normalizeText(assignment.faculty_search);
    return this.faculties.filter((faculty) => {
      if (!faculty?.id || !faculty?.name) {
        return false;
      }
      if (!query) {
        return true;
      }
      return this.normalizeText(faculty.name).includes(query);
    });
  }

  filteredPrograms(assignment: AssignmentRow) {
    const query = this.normalizeText(assignment.program_search);
    return this.academicPrograms.filter((program) => {
      if (!program?.id || !program?.name) {
        return false;
      }
      if (assignment.faculty_ids.length && !assignment.faculty_ids.includes(program.faculty_id)) {
        return false;
      }
      if (!query) {
        return true;
      }
      const label = `${program.name} ${program.faculty ?? ''}`;
      return this.normalizeText(label).includes(query);
    });
  }

  isFacultySelected(assignment: AssignmentRow, facultyId: string) {
    return assignment.faculty_ids.includes(facultyId);
  }

  isProgramSelected(assignment: AssignmentRow, programId: string) {
    return assignment.academic_program_ids.includes(programId);
  }

  toggleAssignmentFaculty(assignment: AssignmentRow, facultyId: string, checked: boolean) {
    if (checked) {
      if (!assignment.faculty_ids.includes(facultyId)) {
        assignment.faculty_ids = [...assignment.faculty_ids, facultyId];
      }
      return;
    }

    assignment.faculty_ids = assignment.faculty_ids.filter((id) => id !== facultyId);
    assignment.academic_program_ids = assignment.academic_program_ids.filter((programId) => {
      const program = this.academicPrograms.find((item) => item.id === programId);
      return program?.faculty_id !== facultyId;
    });
  }

  toggleAssignmentProgram(assignment: AssignmentRow, programId: string, checked: boolean) {
    const program = this.academicPrograms.find((item) => item.id === programId);
    if (!program) {
      return;
    }

    if (checked) {
      if (!assignment.academic_program_ids.includes(programId)) {
        assignment.academic_program_ids = [...assignment.academic_program_ids, programId];
      }
      if (program.faculty_id && !assignment.faculty_ids.includes(program.faculty_id)) {
        assignment.faculty_ids = [...assignment.faculty_ids, program.faculty_id];
      }
      return;
    }

    assignment.academic_program_ids = assignment.academic_program_ids.filter((id) => id !== programId);
  }

  selectAllFaculties(assignment: AssignmentRow) {
    assignment.faculty_ids = [...new Set(this.filteredFaculties(assignment).map((faculty) => faculty.id))];
  }

  clearFaculties(assignment: AssignmentRow) {
    assignment.faculty_ids = [];
    assignment.academic_program_ids = [];
  }

  selectAllPrograms(assignment: AssignmentRow) {
    const programs = this.filteredPrograms(assignment);
    const nextProgramIds = new Set(assignment.academic_program_ids);
    const nextFacultyIds = new Set(assignment.faculty_ids);

    for (const program of programs) {
      nextProgramIds.add(program.id);
      if (program.faculty_id) {
        nextFacultyIds.add(program.faculty_id);
      }
    }

    assignment.academic_program_ids = [...nextProgramIds];
    assignment.faculty_ids = [...nextFacultyIds];
  }

  clearPrograms(assignment: AssignmentRow) {
    assignment.academic_program_ids = [];
  }

  assignmentSummary(assignment: AssignmentRow) {
    const facultyCount = assignment.faculty_ids.length;
    const programCount = assignment.academic_program_ids.length;

    if (!facultyCount && !programCount) {
      return 'Global / derivada';
    }

    if (programCount && facultyCount) {
      return `${facultyCount} facultades y ${programCount} programas`;
    }

    if (facultyCount) {
      return `${facultyCount} facultades`;
    }

    return `${programCount} programas`;
  }

  private syncSelectionsAfterReload() {
    if (this.selectedUserId) {
      const user = this.users.find((item) => item.id === this.selectedUserId);
      if (user) {
        this.selectUser(user);
      }
    }
    if (this.selectedRoleId) {
      const role = this.roles.find((item) => item.id === this.selectedRoleId);
      if (role) {
        this.selectRole(role);
      }
    }
  }

  private emptyUserForm() {
    return {
      id: '',
      username: '',
      display_name: '',
      email: '',
      password: '',
      is_active: true,
    };
  }

  private emptyAssignment(): AssignmentRow {
    return {
      role_id: '',
      faculty_ids: [],
      academic_program_ids: [],
      is_active: true,
      faculty_search: '',
      program_search: '',
    };
  }

  private emptyRoleForm() {
    return {
      id: '',
      code: '',
      name: '',
      is_active: true,
      is_editable: true,
    };
  }

  private buildLoadError(label: string, error: any) {
    return error?.error?.message ?? `No se pudo cargar ${label}.`;
  }

  private mapAssignmentsToRows(assignments: any[]): AssignmentRow[] {
    const grouped = new Map<string, AssignmentRow>();
    for (const assignment of assignments) {
      const key = `${assignment.role_id ?? ''}|${assignment.is_active !== false ? '1' : '0'}`;
      if (!grouped.has(key)) {
        grouped.set(key, {
          role_id: assignment.role_id ?? '',
          faculty_ids: [],
          academic_program_ids: [],
          is_active: assignment.is_active ?? true,
          faculty_search: '',
          program_search: '',
        });
      }
      const row = grouped.get(key)!;
      if (assignment.faculty_id && !row.faculty_ids.includes(assignment.faculty_id)) {
        row.faculty_ids = [...row.faculty_ids, assignment.faculty_id];
      }
      if (
        assignment.academic_program_id &&
        !row.academic_program_ids.includes(assignment.academic_program_id)
      ) {
        row.academic_program_ids = [...row.academic_program_ids, assignment.academic_program_id];
      }
    }
    return [...grouped.values()];
  }

  private expandAssignmentRow(assignment: AssignmentRow) {
    const rows: Array<{
      role_id: string;
      faculty_id: string | null;
      academic_program_id: string | null;
      is_active: boolean;
    }> = [];
    const selectedFacultyIds = [...new Set(assignment.faculty_ids)];
    const selectedProgramIds = [...new Set(assignment.academic_program_ids)];

    if (selectedFacultyIds.length === 0 && selectedProgramIds.length === 0) {
      return [
        {
          role_id: assignment.role_id,
          faculty_id: null,
          academic_program_id: null,
          is_active: assignment.is_active,
        },
      ];
    }

    for (const programId of selectedProgramIds) {
      const program = this.academicPrograms.find((item) => item.id === programId);
      rows.push({
        role_id: assignment.role_id,
        faculty_id: program?.faculty_id ?? null,
        academic_program_id: programId,
        is_active: assignment.is_active,
      });
    }

    const facultyIdsCoveredByPrograms = new Set(
      selectedProgramIds
        .map((programId) => this.academicPrograms.find((item) => item.id === programId)?.faculty_id ?? null)
        .filter((item): item is string => Boolean(item)),
    );

    for (const facultyId of selectedFacultyIds) {
      if (facultyIdsCoveredByPrograms.has(facultyId)) {
        continue;
      }
      rows.push({
        role_id: assignment.role_id,
        faculty_id: facultyId,
        academic_program_id: null,
        is_active: assignment.is_active,
      });
    }

    return rows;
  }

  private beginLoad() {
    this.pendingLoads += 1;
    this.isLoading = true;
  }

  private endLoad() {
    this.pendingLoads = Math.max(0, this.pendingLoads - 1);
    this.isLoading = this.pendingLoads > 0;
    this.cdr.detectChanges();
  }

  private normalizeText(value: string | null | undefined) {
    return (value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .trim();
  }

  private normalizeRolePermissionIds(rolePermissions: any[]) {
    const next = new Set<string>((rolePermissions ?? []).map((permission: any) => permission.id));
    for (const permission of rolePermissions ?? []) {
      if (!permission?.parent_window_code) {
        continue;
      }
      const parent = this.permissions.find((item) => item.code === permission.parent_window_code);
      if (parent?.id) {
        next.add(parent.id);
      }
    }
    return next;
  }
}
