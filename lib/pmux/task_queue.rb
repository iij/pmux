module Pmux
  class TaskQueue < Array
    def inject_tasks tasks
      self.concat tasks
    end

    def delete_job job_id
      delete_if {|item| item[:job_id] == job_id}
    end
  end
end
